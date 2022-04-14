#define _XOPEN_SOURCE 500
#define _GNU_SOURCE
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <err.h>
#include <stdbool.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <semaphore.h>
#include <string.h>
#include <time.h>
#include <sched.h>
#include <dlfcn.h>

#include "lua_head.h"

// Include for debugging.
#include "show_stack.h"

#include "twinmap.h"

#define MIN_TASKS 64
// This is a uint16_t.  Maybe make this smaller.
#define MAX_TASKS 65536
#define MIN_CONTROL_BUFFER_SIZE (1<<16)
#define MIN_CLIENT_BUFFER_SIZE (1<<16)
#define DEFAULT_CONTROL_BUFFER_SIZE (1<<18)
#define DEFAULT_CLIENT_BUFFER_SIZE (1<<18)

// Wake 20 times per second.
#define HOUSEKEEPER_WAKE_USEC 50000
#define SECONDS_HOUSEKEEPER (1000000/HOUSEKEEPER_WAKE_USEC)

#define INLINE_CBUF_CODE
#include "cbuf.h"

// Control message types
#define GET_TASK_INDEX 0
#define CREATE_TASK 1
#define THREAD_EXITS 2
#define REQUEST_SHUTDOWN 3
#define SHOW_STATUS 4

// Client message types
#define NORMAL_CLIENT_MSG 0
#define SYSMSG 8192
#define TASK_CREATE SYSMSG
#define TASK_BAD_CREATE (SYSMSG+1)
#define TASK_EXIT (SYSMSG+2)
#define TASK_FAILURE (SYSMSG+3)
#define TASK_CANCEL (SYSMSG+4)

#define MAX_PRIVATE_FLAG 30
#define IMMUNITY (1U<<31)

#define MAX_MSG_TYPE 4095
#define CHANNEL_COUNT 24

#define CHANNEL_MASK ((1<<CHANNEL_COUNT)-1)

#define SUBSCRIBE_SHIFT 24
#define RECEIVE_CHILD_EXITS (1<<SUBSCRIBE_SHIFT)
#define RECEIVE_ALL_TASK_EXITS (2<<SUBSCRIBE_SHIFT)
#define RECEIVE_NEW_NAMED_TASKS (4<<SUBSCRIBE_SHIFT)
#define RECEIVE_ANY_NEW_TASKS (8<<SUBSCRIBE_SHIFT)
#define RECEIVE_CREATE_FAILURES (16<<SUBSCRIBE_SHIFT)
#define RECEIVE_ANY_RESULTS (32<<SUBSCRIBE_SHIFT)
struct announcements {
    const char *name;
    int flags;
} announcements[] = {
    { "child_task_exits", RECEIVE_CHILD_EXITS },
    { "any_task_exits", RECEIVE_ALL_TASK_EXITS },
    { "new_named_task", RECEIVE_NEW_NAMED_TASKS },
    { "any_new_task", RECEIVE_ANY_NEW_TASKS },
    { "create_failures", RECEIVE_CREATE_FAILURES },
    { "any_results", RECEIVE_ANY_RESULTS | RECEIVE_ALL_TASK_EXITS },
    { NULL, 0 }
};
// Unused bits: 64, and 128

// Align message lengths on double words.
#define ALIGN(N) ((N)+7 & ~7)

static __thread lua_State *L;

static __thread uint16_t my_index;

static char *housekeeper_name = "HouseKeeper";
static char *maintask_name = ":main:";

static sem_t task_running_sem;

struct task_name {
    uint32_t use_count;
    char name[0];
};

struct task {
    uint32_t nonce, last_active_nonce;
    uint16_t parent_index;
    uint32_t parent_nonce;
    uint32_t private_flags;
    uint32_t subscriptions;
    uint32_t queue_in_use;
    struct circbuf incoming_queue;
    uint8_t *incoming_store;
    pthread_t thread;
    sem_t housekeeper_pending;
    sem_t incoming_sem;
    pthread_mutex_t incoming_mutex;
    int query_index;
    uint32_t query_extra;
    struct task_name *task_name;
    lua_State *my_state;
};

int num_tasks;
struct task *tasks;

#define MAX_GLOBAL_FLAG 31
static uint32_t global_flags;

pthread_t housekeeper_thread;

static char *bad_msg = "%s: Bad message";

static size_t control_channel_size;
static uint8_t *control_channel_store;
static struct circbuf control_channel_buf;
static sem_t control_channel_sem;
static pthread_mutex_t control_channel_mutex;

struct message {
    uint32_t size;
    struct task_name *sender_name;
    uint32_t nonce;
    uint16_t sender;
    uint16_t type;
    uint8_t payload[0];
};

extern int freezer_thaw_buffer(lua_State *);
extern int freezer_freeze(lua_State *L);

static void sigexit(lua_State *L, lua_Debug *ar)
{
  (void)ar;  /* unused arg. */
  lua_sethook(L, NULL, 0, 0);
  lua_pushstring(L, "interrupted!");
  lua_error(L);
}

static void sigusr1_handler(int sig)
{
    lua_sethook(L, sigexit, LUA_MASKCALL | LUA_MASKRET | LUA_MASKCOUNT, 1);
}

// This may (likely) leave the lua state inconsistent.
// Cleanup may very well crash.
static void sigusr2_handler(int sig)
{
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
    pthread_cancel(pthread_self());
}

LUAFN(traceback)
{
    if (!lua_isstring(L, 1))  /* 'message' not a string? */
	return 1;  /* keep it intact */
    lua_getfield(L, LUA_GLOBALSINDEX, "debug");
    if (!lua_istable(L, -1)) {
	lua_pop(L, 1);
	return 1;
    }
    lua_getfield(L, -1, "traceback");
    if (!lua_isfunction(L, -1)) {
	lua_pop(L, 2);
	return 1;
    }
    lua_pushvalue(L, 1);  /* pass error message */
    lua_pushinteger(L, 2);  /* skip this function and traceback */
    lua_call(L, 2, 1);  /* call debug.traceback */
    return 1;
}

static void purge_incoming_queue(struct task *task)
{
    struct message *msg;

    while (cb_occupied(&task->incoming_queue) > 0) {
	msg = (void *)(cb_head(&task->incoming_queue) + task->incoming_store);
	uint32_t msgmax = cb_occupied(&task->incoming_queue);
	// Sanity.  Should never fail.
	if (msgmax < sizeof(struct message))
	    errx(1, bad_msg, "client");
	if (msg->sender_name &&
	    __atomic_sub_fetch(&msg->sender_name->use_count, 1,
			       __ATOMIC_SEQ_CST) == 0)
	    free(msg->sender_name);
	size_t size = msg->size;
	cb_release(&task->incoming_queue, ALIGN(sizeof(struct message)+size));
    }
}

int send_ctl_msg(uint32_t command, const char *payload, uint32_t size)
{
    uint32_t msgsize = ALIGN(sizeof(struct message) + size);
    bool success = false;
    pthread_mutex_lock(&control_channel_mutex);
    if (cb_available(&control_channel_buf) >= msgsize) {
	struct message *msg =
	    (void *)(cb_tail(&control_channel_buf) + control_channel_store);
	msg->sender = my_index;
	msg->nonce = tasks[my_index].nonce;
	msg->type = command;
	msg->size = size;
	if (size > 0)
	    memcpy(msg->payload, payload, size);
	cb_produce(&control_channel_buf, msgsize);
	success = true;
    }
    pthread_mutex_unlock(&control_channel_mutex);
    if (!success) return -1;
    sem_post(&control_channel_sem);
    return 0;
}

int send_client_msg(struct task *task,
		    uint32_t type, const char *payload, uint32_t size)
{
    uint32_t msgsize = ALIGN(sizeof(struct message) + size);
    bool success = false;
    pthread_mutex_lock(&task->incoming_mutex);
    if (__atomic_add_fetch(&task->queue_in_use, 1, __ATOMIC_SEQ_CST) == 2 &&
	cb_available(&task->incoming_queue) >= msgsize) {
	struct message *msg =
	    (void *)(cb_tail(&task->incoming_queue) + task->incoming_store);
	msg->sender = my_index;
	msg->sender_name = tasks[my_index].task_name;
	if (msg->sender_name)
	    (void)__atomic_add_fetch(&msg->sender_name->use_count,
				     1, __ATOMIC_SEQ_CST);
	msg->nonce = tasks[my_index].nonce;
	msg->type = type;
	msg->size = size;
	if (size > 0)
	    memcpy(msg->payload, payload, size);
	cb_produce(&task->incoming_queue, msgsize);
	success = true;
    }
    if (__atomic_sub_fetch(&task->queue_in_use, 1, __ATOMIC_SEQ_CST) == 0 &&
	tasks->incoming_store) {
	purge_incoming_queue(task);
	free_twinmap(task->incoming_store,
		     task->incoming_queue.size);
	tasks->incoming_store = 0;
    }
    pthread_mutex_unlock(&task->incoming_mutex);
    if (!success) return -1;
    sem_post(&task->incoming_sem);
    return 0;
}

// These need to be shared between new_thread and the cancellation handler.
static __thread bool show_errors;
static __thread bool show_exits;

void cancellation_handler(void *dummy)
{
    struct task *task = &tasks[my_index];
    if (show_exits || show_errors)
	if (task->task_name)
	    fprintf(stderr, "Task %s has been cancelled\n",
		    task->task_name->name);
	else
	    fprintf(stderr, "Task %d,%d has been cancelled\n",
		    my_index, task->nonce);

    for (int i=0; i < num_tasks; i++)
	if (tasks[i].nonce != 0 &&
	    (i==task->parent_index && tasks[i].nonce == task->parent_nonce &&
	     tasks[i].subscriptions & RECEIVE_CHILD_EXITS ||
	     tasks[i].subscriptions & RECEIVE_ALL_TASK_EXITS))
	    send_client_msg(&tasks[i], TASK_CANCEL, "", 0);

    tasks[my_index].nonce = 0;
    if (send_ctl_msg(THREAD_EXITS, "C", 1))
	errx(1, "Can't send task exit message.");
    lua_close(L);    // Possibly crashy.  Leaks otherwise.
}

static void *new_thread(void *luastate)
{
    int previous_cancel;
    bool succeeded = false;
    bool send_results;
    // Race?
    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &previous_cancel);
    L = luastate;

    // Be sure we have all the necessaries.
    luaL_openlibs(L);
    void *repl = dlsym(RTLD_DEFAULT, "lua_repl");
    if (repl) {
	lua_pushcfunction(L, repl);
	lua_setglobal(L, "lua_repl");
    }

    // Stack:
    //   2 - my index
    //   1 - program description table
    my_index = lua_tointeger(L, -1);
    struct task *task = &tasks[my_index];
    lua_pop(L, 1);

    // Let everyone know this isn't GCable.
    __atomic_add_fetch(&task->queue_in_use, 1, __ATOMIC_SEQ_CST);

    if (++task->last_active_nonce == 0)
	// Nonce zero means unused, so skip over it.
	task->last_active_nonce = 1;
    // Set exit status printing as asked.
    lua_getfield(L, -1, "show_errors");
    show_errors = lua_toboolean(L, -1);
    lua_pop(L, 1);
    lua_getfield(L, -1, "show_exits");
    show_exits = lua_toboolean(L, -1);
    lua_pop(L, 1);
    lua_getfield(L, -1, "send_results");
    send_results = lua_toboolean(L, -1);
    lua_pop(L, 1);

    // Fetch the program.  (i.e. function, file, or string chunk)
    lua_getfield(L, -1, "program");
    int rc = 0;
    if (lua_type(L, -1) == LUA_TSTRING &&
	lua_objlen(L, -1) > 1) {
	size_t len;
	const char *progtext=lua_tolstring(L, -1, &len);
	if (progtext[0] == ':')
	    rc = luaL_loadfile(L, &progtext[1]);
	else
	    rc = luaL_loadbuffer(L, progtext, len, "new_thread()");
	lua_remove(L, -2);
    }
    // If we don't have a function now, then caller made a booboo.
    if (!lua_isfunction(L, -1)) {
	if (rc == 0)
	    lua_pushfstring(L, "Invalid program type: %s",
			    lua_typename(L, lua_type(L, -1)));
	// Wake housekeeper even though program is bogus.
	sem_post(&task_running_sem);
	goto bugout;
    }

    // Program is established, so tell housekeeper we're running.
    task->nonce = task->last_active_nonce;
    sem_post(&task_running_sem);

    // Setup arguments from program description.
    int arg_count = 0;
    while (1) {
	lua_rawgeti(L, 1, ++arg_count);
	if (lua_isnil(L, -1)) {
	    lua_pop(L, 1);
	    arg_count--;
	    break;
	}
    }
    // We're done with the program description now.
    lua_remove(L, 1);

    for (int i=0; i < num_tasks; i++)
	if (tasks[i].nonce != 0 &&
	    (tasks[i].subscriptions & RECEIVE_NEW_NAMED_TASKS &&
	     tasks[my_index].task_name ||
	     tasks[i].subscriptions & RECEIVE_ANY_NEW_TASKS))
	    send_client_msg(&tasks[i], TASK_CREATE, "", 0);

    int pcall_fails = 0;
    pthread_cleanup_push(cancellation_handler, NULL);
    pthread_setcancelstate(previous_cancel, NULL);
    // Add error handler
    lua_pushcfunction(L, LUAFN_NAME(traceback));
    lua_insert(L, 1);
    // Call user program
    pcall_fails = lua_pcall(L, arg_count, LUA_MULTRET, 1);
    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
    pthread_cleanup_pop(0);

    if (!pcall_fails) {
	// Pack up return values
	int retvals = lua_gettop(L) - 1;
	lua_newtable(L);
	lua_insert(L, 2);
	for (int i = retvals; i > 0; i--)
	    lua_rawseti(L, 2, i);
	lua_settop(L, 2);
	lua_getglobal(L, "string");
	lua_getfield(L, -1, "dump");
	lua_pushcclosure(L, freezer_freeze, 1);
	lua_insert(L, 2);
	lua_pop(L, 1);
	// Serialization might fail, so that's still a loss to us.
	succeeded = !lua_pcall(L, 1, 1, 0);
    }

bugout:
    if (succeeded && show_exits)
	if (task->task_name)
	    fprintf(stderr, "Task %s exits.\n", task->task_name->name);
	else
	    fprintf(stderr, "Task %d,%d exits.\n", my_index, task->nonce);

    if (!succeeded && (show_errors || show_exits))
	if (task->task_name)
	    fprintf(stderr, "Task %s failed: %s\n",
		    task->task_name->name, lua_tostring(L, -1));
	else
	    fprintf(stderr, "Task %d,%d failed: %s\n",
		    my_index, task->nonce, lua_tostring(L, -1));

    size_t pack_len = lua_objlen(L, -1);
    for (int i=0; i < num_tasks; i++)
	if (tasks[i].nonce != 0 &&
	    (i==task->parent_index && tasks[i].nonce == task->parent_nonce &&
	     (tasks[i].subscriptions & RECEIVE_CHILD_EXITS || send_results) ||
	     tasks[i].subscriptions & RECEIVE_ALL_TASK_EXITS))
	    if (!succeeded ||
		i == task->parent_index && send_results ||
		(tasks[i].subscriptions & RECEIVE_ANY_RESULTS) ==
		RECEIVE_ANY_RESULTS)
		send_client_msg(&tasks[i],
				succeeded ? TASK_EXIT : TASK_FAILURE,
				lua_tostring(L, -1), pack_len);
	    else
		send_client_msg(&tasks[i], TASK_EXIT, "", 0);

    tasks[my_index].nonce = 0;
    if (send_ctl_msg(THREAD_EXITS, succeeded ? "S" : "F", 1))
	errx(1, "Can't send task exit message.");
    lua_close(L);
    return NULL;
}

static int create_task(uint8_t *taskdescr, int size,
		       uint16_t sender, uint32_t sender_nonce)
{
    lua_State *newstate;
    struct task *task = NULL;
    struct task *sender_task = &tasks[sender];
    static unsigned int last_task_allocated;
    bool show_errors;

    // Create_task is invoked from housekeeper only, so L refers to
    // housekeeper's dictionary store rather than client's lua_State.

    int freetask = -1;
    // Look for a free slot O(n).
    int next_task_allocated = last_task_allocated;
    for (int i = 1; i < num_tasks; i++) {
	if (++next_task_allocated == num_tasks)
	    next_task_allocated = 1;
	// Previously used task is available if child has cleared
	// the nonce and housekeeper has freed the queue storage.
	if (tasks[next_task_allocated].nonce == 0 &&
	    tasks[next_task_allocated].incoming_store == 0) {
	    freetask = next_task_allocated;
	    break;
	}
    }
    newstate = luaL_newstate();
    if (freetask < 0) {
	lua_pushstring(newstate, "No tasks available.");
	show_errors = true;  // Bad underallocation.  Always complain.
	goto bugout;
    }

    // Unpack task description
    lua_pushcfunction(newstate, freezer_thaw_buffer);
    lua_pushlightuserdata(newstate, (void *)taskdescr);
    lua_pushinteger(newstate, size);
    // This shouldn't happen unless freezer is broken.  If so, panic!
    if (lua_pcall(newstate, 2,1,0))
	errx(1, "Freezer decode failed: %s", lua_tostring(newstate, -1));
    task = &tasks[freetask];
    // pthread_cancel needs this for cleanup.
    task->my_state = newstate;
    task->private_flags = 0;
    task->subscriptions = 0;

    lua_getfield(newstate, -1, "show_errors");
    show_errors |= lua_toboolean(newstate, -1);
    lua_pop(newstate, 1);

    // Set task name if given one.
    lua_getfield(newstate, -1, "task_name");
    task->task_name = NULL;
    if (!lua_isnil(newstate, -1)) {
	if (lua_type(newstate, -1) != LUA_TSTRING) {
	    lua_pushstring(newstate, "Invalid task name");
	    goto bugout;
	}
	const char *namestr = lua_tostring(newstate,-1);
	lua_getfield(L, 1, namestr);
	bool unused = lua_isnil(L, -1);
	lua_settop(L, 2);
	if (!unused) {
	    lua_pushfstring(newstate, "Task name %s in use", namestr);
	    goto bugout;
	}
	int namelen = strlen(namestr);
	task->task_name = malloc(sizeof(struct task_name) + namelen+1);
	task->task_name->use_count = 1;
	memcpy(task->task_name->name, namestr, namelen+1);
    }
    lua_pop(newstate, 1);

    //  Create incoming message queue.
    lua_getfield(newstate, -1, "queue_size");
    int queue_size;
    if (lua_isnil(newstate, -1))
	queue_size = DEFAULT_CLIENT_BUFFER_SIZE;
    else if (lua_isnumber(newstate, -1))
	queue_size = lua_tointeger(newstate, -1);
    else {
	lua_pushstring(newstate, "Bad queue size");
	goto bugout;
    }
    lua_pop(newstate, 1);
    if (queue_size < MIN_CLIENT_BUFFER_SIZE)
	queue_size = MIN_CLIENT_BUFFER_SIZE;
    size_t qsize = queue_size;
    task->incoming_store = allocate_twinmap(&qsize);
    cb_init(&task->incoming_queue, qsize);
    sem_init(&task->incoming_sem, 0, 0);
    pthread_mutex_init(&task->incoming_mutex, NULL);

    task->parent_index = sender;
    // Is this still valid.  What if this becomes zero?
    task->parent_nonce = sender_nonce;
    // Requestor waits on this for replies from housekeeper.
    sem_init(&task->housekeeper_pending, 0, 0);
    lua_pushinteger(newstate, freetask);
    if (pthread_create(&task->thread, NULL, new_thread,
		       (void *)newstate) < 0) {
	lua_pushstring(newstate, "Can't create thread");
	goto bugout;
    }
    last_task_allocated = next_task_allocated;
    // Save task in dictionary.
    if (task->task_name) {
	lua_pushstring(L, task->task_name->name);
	lua_pushinteger(L, freetask);
	lua_rawset(L, 1);
    }

    return freetask;
bugout:
    if (show_errors)
	fprintf(stderr, "Task creation failed: %s\n",
		lua_tostring(newstate, -1));
    if (sender_task->nonce != 0 &&
	sender_task->subscriptions & RECEIVE_CREATE_FAILURES) {
	size_t len;
	const char *errmsg = lua_tolstring(newstate, -1, &len);
	send_client_msg(sender_task, TASK_BAD_CREATE, errmsg, len);
    }
    if (task) {
	if (task->incoming_store)
	    free_twinmap(task->incoming_store,
			 task->incoming_queue.size);
	free(task->task_name);
    }

    lua_close(newstate);
    return -1;
}

static bool initialized;

static void kill_stragglers()
{
    int found_some = 0;
    for (int i = 1; i < num_tasks; i++)
	if (tasks[i].nonce != 0) {
	    found_some++;
	    pthread_kill(tasks[i].thread, SIGUSR2);
	}
    if (found_some)
	fprintf(stderr, "Stragglers: %d\n", found_some);
}

struct task_description {
    uint8_t *description;
    int length;
};

static void *housekeeper(void *dummy)
{
    struct message *msg;
    int task_count = 0;
    int finished = 0, failed = 0, cancelled = 0;
    // Kill stragglers on shutdown after 1/4 sec.
    int straggler_delay =  SECONDS_HOUSEKEEPER / 4;

    if ((L = luaL_newstate()) == NULL)
	err(1, housekeeper_name);

    lua_newtable(L); // Task dictionary;

    // Save the task for the main process.
    lua_pushstring(L, maintask_name);
    lua_pushinteger(L, 0);
    lua_rawset(L, 1);

    bool shutdown = false;
    global_flags = 0;

    sem_init(&task_running_sem, 0, 0);

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    while (1) {
	if (sem_timedwait(&control_channel_sem, &ts) < 0) {
	    if (shutdown && --straggler_delay <= 0)
		kill_stragglers();

	    if ((ts.tv_nsec += 1000L*HOUSEKEEPER_WAKE_USEC) > 1000000000) {
		ts.tv_sec += ts.tv_nsec / 1000000000;
		ts.tv_nsec = ts.tv_nsec % 1000000000;
	    }
	    continue;
	}
	msg = (void *)(cb_head(&control_channel_buf) + control_channel_store);
	size_t msgmax = cb_occupied(&control_channel_buf);
	if (msgmax < sizeof(struct message))
	    errx(1, bad_msg, housekeeper_name);
	int sender = msg->sender;
	size_t size = msg->size;
	switch(msg->type) {
	case GET_TASK_INDEX:
	    lua_pushlstring(L, (void *)msg->payload, size);
	    cb_release(&control_channel_buf,
		       ALIGN(sizeof(struct message)+size));
	    if (shutdown)
		tasks[sender].query_index = -1;
	    else {
		lua_rawget(L, 1);
		if (lua_isnil(L, -1))
		    tasks[sender].query_index = -1;
		else {
		    tasks[sender].query_index = lua_tointeger(L, -1);
		    tasks[sender].query_extra =
			tasks[tasks[sender].query_index].nonce;
		}
	    }
	    sem_post(&tasks[sender].housekeeper_pending);
	    break;
	case CREATE_TASK: {
	    if (shutdown)
		tasks[sender].query_index = -1;
	    else {
		struct task_description *task_descr = (void *)msg->payload;
		int child_ix = create_task(task_descr->description,
					   task_descr->length,
					   sender, msg->nonce);
		tasks[sender].query_index = child_ix;
		if (child_ix > 0) {
		    sem_wait(&task_running_sem);
		    tasks[sender].query_extra = tasks[child_ix].nonce;
		    task_count++;
		}
	    }
	    cb_release(&control_channel_buf,
		       ALIGN(sizeof(struct message)+size));
	    sem_post(&tasks[sender].housekeeper_pending);
	    break;
	}
	case SHOW_STATUS: {
	    bool show_anon = msg->payload[0];
	    cb_release(&control_channel_buf,
		       ALIGN(sizeof(struct message)+size));
	    fprintf(stderr, "\nRunning: %d / Finished: %d / "
		    "Failed: %d / Cancelled: %d\n\n",
		    task_count+1, finished, failed, cancelled);
	    int anon = 0;
	    for (int i = 0; i < num_tasks; i++) {
	    	if (tasks[i].nonce != 0) {
		    if (tasks[i].task_name)
			fprintf(stderr, "     %d, %s\n", i,
				tasks[i].task_name->name);
		    else if (show_anon)
			fprintf(stderr, "   * %d, %d \n", i, tasks[i].nonce);
		    else
			anon++;
	    	}
	    }
	    if (anon > 0)
		fprintf(stderr, "   + %d anonymous\n", anon);
	    fprintf(stderr, "\n");
	}
	    break;
	case THREAD_EXITS: {
		char exit_type = msg->payload[0];
		switch(exit_type) {
		case 'S': finished++; break;
		case 'F': failed++; break;
		case 'C': cancelled++; break;
	    }
	}
	    cb_release(&control_channel_buf,
		       ALIGN(sizeof(struct message)+size));
	    tasks[sender].private_flags=0;
	    tasks[sender].subscriptions=0;
	    pthread_join(tasks[sender].thread, NULL);
	    struct task_name *oldname = tasks[sender].task_name;
	    // Toss message queue store if GCable.
	    if (__atomic_sub_fetch(&tasks[sender].queue_in_use, 1,
				   __ATOMIC_SEQ_CST) == 0 &&
		tasks->incoming_store) {
		purge_incoming_queue(&tasks[sender]);
		free_twinmap(tasks[sender].incoming_store,
			     tasks[sender].incoming_queue.size);
		tasks[sender].incoming_store = 0;
	    }
	    if (oldname) {
		lua_pushnil(L);
		lua_setfield(L, 1, oldname->name);
		if (__atomic_sub_fetch(&oldname->use_count, 1,
				       __ATOMIC_SEQ_CST) == 0) {
		    tasks[sender].task_name = NULL;
		    free(oldname);
		}
	    }
	    if (--task_count <= 0 && shutdown)
		goto fini;
	    break;
	case REQUEST_SHUTDOWN:
	    cb_release(&control_channel_buf,
		       ALIGN(sizeof(struct message)+size));
	    if (sender != 0) {
		sem_post(&tasks[sender].housekeeper_pending);
		break;
	    }
	    shutdown = true;
	    if (task_count == 0)
		goto fini;
	    for (int i = 1; i < num_tasks; i++)
		if (tasks[i].nonce != 0)
		    pthread_kill(tasks[i].thread, SIGUSR1);
	    break;
	default:
	    errx(1, bad_msg, housekeeper_name);
	}
	// Ditch everything but the task dictionary.
	lua_settop(L, 1);
    }

fini:
    sem_post(&tasks[0].housekeeper_pending);
    lua_close(L);
    signal(SIGUSR1, SIG_DFL);
    signal(SIGUSR2, SIG_DFL);
    return NULL;
}

/******************************/
/* Internal utility functions */
/******************************/

#define ASSURE_INITIALIZED					\
    if (!initialized) initialize(L,				\
				 0,				\
				 DEFAULT_CONTROL_BUFFER_SIZE,	\
				 DEFAULT_CLIENT_BUFFER_SIZE)
#define TASK_FAIL_IF_UNINITIALIZED		\
    do if (!initialized) {			\
	    lua_pushnil(L);			\
	    lua_pushstring(L, "Uninitialized"); \
	    return 2; } while(0)

static int initialize(lua_State *L,
		      unsigned int task_limit,
		      size_t control_channel_size_wanted,
		      size_t main_incoming_channel_size)
{
    if (initialized)
	luaL_error(L, "TASKMAN is already initialized");
    initialized = true;

    pthread_mutex_init(&control_channel_mutex, NULL);
    sem_init(&control_channel_sem,0,0);

    struct sigaction action;
    action.sa_handler = sigusr1_handler;
    action.sa_flags = 0;
    sigemptyset(&action.sa_mask);
    sigaction(SIGUSR1, &action, NULL);
    action.sa_handler = sigusr2_handler;
    sigaction(SIGUSR2, &action, NULL);

    if (task_limit > MAX_TASKS) task_limit = MAX_TASKS;
    num_tasks = task_limit < MIN_TASKS ? MIN_TASKS : task_limit;
    tasks = calloc(num_tasks, sizeof(struct task));

    control_channel_size = control_channel_size_wanted;
    if (control_channel_size < MIN_CONTROL_BUFFER_SIZE)
	control_channel_size = MIN_CONTROL_BUFFER_SIZE;
    control_channel_store = allocate_twinmap(&control_channel_size);
    cb_init(&control_channel_buf, control_channel_size);
    if (main_incoming_channel_size < MIN_CLIENT_BUFFER_SIZE)
	main_incoming_channel_size = MIN_CLIENT_BUFFER_SIZE;
    tasks->incoming_store = allocate_twinmap(&main_incoming_channel_size);
    cb_init(&tasks->incoming_queue, main_incoming_channel_size);

    // Main thread is task 0;
    tasks->thread = pthread_self();
    tasks->task_name =
	malloc(sizeof(struct task_name) + sizeof(maintask_name));
    tasks->task_name->use_count = 1;
    strcpy(tasks->task_name->name, maintask_name);
    tasks->nonce = 1;
    tasks->queue_in_use = 1;
    sem_init(&tasks->housekeeper_pending, 0, 0);

    return pthread_create(&housekeeper_thread, NULL, &housekeeper, NULL);
}

LUAFN(timestamp_name)
{
    char timestr[32];
    struct timespec *ts = lua_touserdata(L, 1);
    char sign =  ts->tv_sec < 0 || ts->tv_nsec < 0;
    snprintf(timestr, 32, "%s%ld.%09ld", sign ? "-" : "",
	     ts->tv_sec < 0 ? -ts->tv_sec : ts->tv_sec,
	     ts->tv_nsec < 0 ? -ts->tv_nsec : ts->tv_nsec);
    lua_pushfstring(L, "timestamp (%s)", timestr);
    return 1;
}

static inline __attribute__((always_inline))
void add_time_delta(struct timespec *newtime, struct timespec *oldtime,
		    double delta)
{
    delta += oldtime->tv_sec + 1E-9L*oldtime->tv_nsec;
    newtime->tv_sec = delta;
    newtime->tv_nsec = 1E9L*(delta - newtime->tv_sec);
}

static inline __attribute__((always_inline))
double time_diff(struct timespec *time1, struct timespec *time2)
{
    return time1->tv_sec - time2->tv_sec +
	1E-9L*(time1->tv_nsec - time2->tv_nsec);
}

struct task_cache_entry {
    uint16_t task_ix;
    uint32_t nonce;
};

static int wait_for_reply(lua_State *L)
{
    struct task *mytask = &tasks[my_index];

    sem_wait(&mytask->housekeeper_pending);
    if (mytask->query_index < 0)
	return 0;
    lua_pushinteger(L, mytask->query_index);
    lua_pushinteger(L, mytask->query_extra);
    return 2;
}

int lookup_task(lua_State *L, const char *name)
{
    int top = lua_gettop(L);
    lua_getfield(L, lua_upvalueindex(1), name);
    if (!lua_isnil(L, -1)) {
	struct task_cache_entry *task_entry = (void *)lua_touserdata(L, -1);
	if (tasks[task_entry->task_ix].nonce == task_entry->nonce) {
	    lua_settop(L, top);
	    return task_entry->task_ix;
	}
    }
    lua_pop(L, 1);
    if (send_ctl_msg(GET_TASK_INDEX, name, strlen(name))) {
	lua_settop(L, top);
	return -1;
    }
    int result = wait_for_reply(L);
    if (result > 0) {
	struct task_cache_entry *task_entry =
	    lua_newuserdata(L, sizeof(struct task_cache_entry));
	result = task_entry->task_ix = lua_tointeger(L, -3);
	task_entry->nonce = lua_tointeger(L, -2);
    } else {
	result = -1;
	lua_pushnil(L);
    }
    lua_setfield(L, lua_upvalueindex(1), name);
    lua_settop(L, top);
    return result;
}

int validate_task(lua_State *L, int ix)
{
    if (lua_isnoneornil(L, ix))
	return my_index;
    if (lua_isnumber(L, ix)) {
	int task_ix = luaL_checkinteger(L, ix);
	if (task_ix == -1) {
	    struct task *task = &tasks[my_index];
	    if (tasks[task->parent_index].nonce == task->parent_nonce)
		return task->parent_index;
	}
	if (task_ix < 0 || task_ix >= num_tasks)
	    return -1;
	if (lua_isnoneornil(L, ix+1)) {
	    if (tasks[task_ix].nonce == 0)
		return -1;
	} else if (luaL_checkinteger(L, ix+1) != tasks[task_ix].nonce)
	    return -1;
	return task_ix;
    }
    return lookup_task(L, luaL_checkstring(L, ix));
}

static int getmsg(lua_State *L)
{
    struct task *task = &tasks[my_index];
    struct message *msg;

    if (cb_occupied(&task->incoming_queue) == 0)
	return 0;
    msg = (void *)(cb_head(&task->incoming_queue) + task->incoming_store);
    uint32_t msgmax = cb_occupied(&task->incoming_queue);
    // Sanity.  Should never fail.
    if (msgmax < sizeof(struct message))
	errx(1, bad_msg, "client");
    int sender = msg->sender;
    struct task *sender_task = &tasks[sender];
    size_t size = msg->size;
    lua_pushlstring(L, size ? (char *)msg->payload : "", size);
    lua_pushinteger(L, msg->type);
    int sender_nonce = msg->nonce;
    struct task_name *sender_name = msg->sender_name;
    cb_release(&task->incoming_queue, ALIGN(sizeof(struct message)+size));
    int retcnt = 4;
    if (sender_name) {
	lua_pushstring(L, sender_name->name);
	if (__atomic_sub_fetch(&sender_name->use_count, 1,
			       __ATOMIC_SEQ_CST) == 0)
	    free(sender_name);
	retcnt++;
    }
    lua_pushinteger(L, sender);
    lua_pushinteger(L, sender_nonce == sender_task->nonce ? sender_nonce : 0);
    return retcnt;
}

static int nosuchtask(lua_State *L)
{
    lua_pushnil(L);
    lua_pushstring(L, "No such task");
    return 2;
}

/****************************************/
/* Explicit initialization and shutdown */
/****************************************/

LUAFN(initialize)
{
    unsigned int task_limit = 0;
    size_t control_channel_size = 0;
    size_t main_incoming_channel_size = 0;

    if (lua_istable(L, 1)) {
	lua_settop(L, 1);
	lua_getfield(L, 1, "task_limit");
	task_limit = lua_tointeger(L, -1);
	lua_getfield(L, 1, "control_channel_size");
	control_channel_size =
	    lua_isnil(L, -1) ? DEFAULT_CONTROL_BUFFER_SIZE
	    : lua_tointeger(L, -1);
	lua_getfield(L, 1, "main_queue_size");
	main_incoming_channel_size =
	    lua_isnil(L, -1) ? DEFAULT_CLIENT_BUFFER_SIZE
	    : lua_tointeger(L, -1);
	lua_settop(L, 0);
    }
    lua_pushboolean(L, initialize(L,
				  task_limit,
				  control_channel_size,
				  main_incoming_channel_size));
    return 0;
}

LUAFN(shutdown)
{
    if (initialized && my_index == 0) {
	send_ctl_msg(REQUEST_SHUTDOWN, "", 0);
	sem_wait(&tasks[my_index].housekeeper_pending);
	pthread_join(housekeeper_thread, NULL);
	purge_incoming_queue(&tasks[0]);
	free_twinmap(tasks[0].incoming_store,
		     tasks[0].incoming_queue.size);
	free(tasks[0].task_name);
	free(tasks);
	free_twinmap(control_channel_store,
		     control_channel_size);
	initialized = false;
    }
    return 0;
}

/*******************/
/* Task management */
/*******************/

LUAFN(create_task)
{
    ASSURE_INITIALIZED;
    lua_settop(L, 1);
    luaL_checktype(L, 1, LUA_TTABLE);
    freezer_freeze(L);
    size_t size = lua_objlen(L, -1);
    struct task_description task_descr =
	{ (uint8_t *)lua_tostring(L, -1), size };
    if (send_ctl_msg(CREATE_TASK, (char *)&task_descr, sizeof(task_descr)))
	return 0;
    return wait_for_reply(L);
}

LUAFN(set_thread_name)
{
#ifdef __linux__
    if (initialized && my_index > 0) {
	if (lua_type(L, 1) != LUA_TSTRING || lua_objlen(L, 1) < 1)
	    luaL_error(L, "Bad thread name");
	char short_name[16];
	strncpy(short_name, lua_tostring(L, 1), 16);
	short_name[15] = 0;
	pthread_setname_np(pthread_self(), short_name);
    }
#endif
    return 0;
}

LUAFN(get_my_name)
{
    ASSURE_INITIALIZED;
    if (tasks[my_index].task_name)
	lua_pushstring(L, tasks[my_index].task_name->name);
    else
	lua_pushnil(L);
    lua_pushinteger(L, my_index);
    lua_pushinteger(L, tasks[my_index].nonce);
    return 3;
}

LUAFN(lookup_task)
{
    TASK_FAIL_IF_UNINITIALIZED;
    int task_ix = validate_task(L, 1);
    if (task_ix < 0)
	return nosuchtask(L);
    lua_pushinteger(L, task_ix);
    lua_pushinteger(L, tasks[task_ix].nonce);
    return 2;
}

LUAFN(interrupt_task)
{
    TASK_FAIL_IF_UNINITIALIZED;
    int signo = lua_toboolean(L, 1) ? 12 : 10;
    int task_ix = validate_task(L, 2);
    if (task_ix != my_index && task_ix > 0 &&
	tasks[task_ix].nonce != 0 &&
	(~tasks[task_ix].private_flags & IMMUNITY || my_index == 0))
	pthread_kill(tasks[task_ix].thread, signo);
    else
	return nosuchtask(L);
    lua_pushinteger(L, task_ix);
    return 1;
}

LUAFN(interrupt_all)
{
    TASK_FAIL_IF_UNINITIALIZED;
    int signo = lua_toboolean(L, 1) ? 12 : 10;
    for (int i = 1; i < num_tasks; i++)
	if (tasks[i].nonce != 0 && i != my_index &&
	    (~tasks[i].private_flags & IMMUNITY || my_index == 0))
	    pthread_kill(tasks[i].thread, signo);
    return 0;
}

LUAFN(cancel_task)
{
    TASK_FAIL_IF_UNINITIALIZED;
    int task_ix = validate_task(L, 1);
    if (task_ix != my_index && task_ix > 0 &&
	tasks[task_ix].nonce != 0 &&
	(~tasks[task_ix].private_flags & IMMUNITY || my_index == 0))
	pthread_cancel(tasks[task_ix].thread);
    else
	return nosuchtask(L);
    lua_pushinteger(L, task_ix);
    return 1;
}

LUAFN(cancel_all)
{
    TASK_FAIL_IF_UNINITIALIZED;
    for (int i = 1; i < num_tasks; i++)
	if (tasks[i].nonce != 0 && i != my_index &&
	    (~tasks[i].private_flags & IMMUNITY || my_index == 0))
	    pthread_cancel(tasks[i].thread);
    return 0;
}

LUAFN(set_cancel)
{
    ASSURE_INITIALIZED;
    int oldstate = -1, oldtype = -1;
    if (initialized && my_index != 0) {
	if (lua_type(L, 1) != LUA_TBOOLEAN && !lua_isnoneornil(L, 1))
	    luaL_error(L, "Bad cancelstate");
	if (lua_type(L, 2) != LUA_TBOOLEAN && !lua_isnoneornil(L, 2))
	    luaL_error(L, "Bad canceltype");
	if (!lua_isnoneornil(L,1))
	    pthread_setcancelstate(lua_toboolean(L, 1) ?
				   PTHREAD_CANCEL_ENABLE
				   : PTHREAD_CANCEL_DISABLE, &oldstate);
	if (!lua_isnoneornil(L,2))
	    pthread_setcanceltype(lua_toboolean(L, 2) ?
				  PTHREAD_CANCEL_DEFERRED
				  : PTHREAD_CANCEL_ASYNCHRONOUS, &oldtype);
    }

    if (oldstate == -1)
	lua_pushnil(L);
    else
	lua_pushboolean(L, oldstate = PTHREAD_CANCEL_ENABLE);

    if (oldtype == -1)
	lua_pushnil(L);
    else
	lua_pushboolean(L, oldstate = PTHREAD_CANCEL_DEFERRED);

    return 2;
}


/*********/
/* Flags */
/*********/

static char *badflag = "Invalid flag %d";

LUAFN(change_private_flag)
{
    ASSURE_INITIALIZED;
    int flag = luaL_checkinteger(L, 1);
    if (flag < 0 || flag > MAX_PRIVATE_FLAG)
	luaL_error(L, badflag, flag);
    int task_ix = validate_task(L, 3);
    if (task_ix < 0)
	return nosuchtask(L);
    __atomic_and_fetch(&tasks[task_ix].private_flags, ~(1<<flag),
		       __ATOMIC_SEQ_CST);
    __atomic_or_fetch(&tasks[task_ix].private_flags,
		      lua_toboolean(L, 2)<<flag,
		      __ATOMIC_SEQ_CST);
    lua_pushinteger(L, task_ix);
    return 1;
}

LUAFN(broadcast_private_flag)
{
    ASSURE_INITIALIZED;
    int flag = luaL_checkinteger(L, 1);
    if (flag < 0 || flag > MAX_PRIVATE_FLAG)
	luaL_error(L, badflag, flag);
    for (int i = 0; i < num_tasks; i++)
	if (i != my_index && tasks[i].nonce != 0) {
	    __atomic_and_fetch(&tasks[i].private_flags, ~(1<<flag),
			       __ATOMIC_SEQ_CST);
	    __atomic_or_fetch(&tasks[i].private_flags,
			      lua_toboolean(L, 2)<<flag,
			      __ATOMIC_SEQ_CST);
	}
    return 0;
}

LUAFN(private_flag)
{
    bool result=false;
    if (initialized) {
	int flag = luaL_checkinteger(L, 1);
	if (flag < 0 || flag > MAX_PRIVATE_FLAG)
	    luaL_error(L, badflag, flag);
	result = tasks[my_index].private_flags & 1<<flag;
    }
    lua_pushboolean(L, result);
    return 1;
}

LUAFN(get_private_flag_word)
{
    lua_pushnumber(L, initialized ? tasks[my_index].private_flags : 0);
    return 1;
}

LUAFN(change_global_flag)
{
    ASSURE_INITIALIZED;
    int flag = luaL_checkinteger(L, 1);
    if (flag < 0 || flag > MAX_GLOBAL_FLAG)
	luaL_error(L, badflag, flag);
    __atomic_and_fetch(&global_flags, ~(1<<flag), __ATOMIC_SEQ_CST);
    __atomic_or_fetch(&global_flags, lua_toboolean(L, 2)<<flag,
		      __ATOMIC_SEQ_CST);
    return 0;
}

LUAFN(global_flag)
{
    bool result=false;
    if (initialized) {
	int flag = luaL_checkinteger(L, 1);
	if (flag < 0 || flag > MAX_GLOBAL_FLAG)
	    luaL_error(L, badflag, flag);
	result = global_flags & 1<<flag;
    }
    lua_pushboolean(L, result);
    return 1;
}

LUAFN(get_global_flag_word)
{
    lua_pushnumber(L, initialized ? global_flags : 0);
    return 1;
}

LUAFN(set_immunity)
{
    if (!initialized)
	return 0;
    int task_ix = -1;
    if (my_index == 0) {
	task_ix = validate_task(L, 2);
    } else if (tasks[0].private_flags & IMMUNITY)
	task_ix = my_index;
    if (task_ix < 0)
	return nosuchtask(L);

    __atomic_and_fetch(&tasks[task_ix].private_flags, ~IMMUNITY,
		       __ATOMIC_SEQ_CST);
    __atomic_or_fetch(&tasks[task_ix].private_flags,
		      lua_toboolean(L, 1) ? IMMUNITY : 0,
		      __ATOMIC_SEQ_CST);
    lua_pushinteger(L, task_ix);
    return 1;
}

/**************/
/* Timestamps */
/**************/


LUAFN(now)
{
    struct timespec *ts;
    ts = lua_newuserdata(L, sizeof(*ts));
    clock_gettime(CLOCK_REALTIME, ts);
    lua_pushvalue(L, lua_upvalueindex(1));
    lua_setmetatable(L, -2);
    return 1;
}

static char badtime[] = "Bad time spec";

LUAFN(sleep)
{
    struct timespec ts, *sleep_until = &ts;
    if (lua_isnumber(L, 1)) {
	clock_gettime(CLOCK_REALTIME, &ts);
	add_time_delta(&ts, &ts, lua_tonumber(L, 1));
    } else if (lua_getmetatable(L, 1) &&
	       lua_rawequal(L, -1, lua_upvalueindex(1)))
	sleep_until = lua_touserdata(L, -2);
    else
	luaL_error(L, badtime);

    lua_pushboolean(L, clock_nanosleep(CLOCK_REALTIME, TIMER_ABSTIME,
                                       sleep_until, NULL) == 0);
    struct timespec timenow;
    clock_gettime(CLOCK_REALTIME, &timenow);
    lua_pushnumber(L, time_diff(&timenow, sleep_until)); return 2;
}

LUAFN(time_difference)
{
    struct timespec *time1 = lua_touserdata(L, 1);
    struct timespec *time2;
    if (lua_isnumber(L, 2)) {
       time2 = lua_newuserdata(L, sizeof(*time2));
       add_time_delta(time2, time1, -lua_tonumber(L, 2));
       lua_getmetatable(L, 1);
       lua_setmetatable(L, -2);
       return 1;
    }
    lua_getmetatable(L, 1);
    if (!lua_getmetatable(L, 2) || !lua_rawequal(L, -1, -2))
	luaL_error(L, badtime);
    time2 = lua_touserdata(L, 2);
    lua_pushnumber(L, time_diff(time1, time2));
    return 1;
}

LUAFN(time_negate)
{
    struct timespec *time1 = lua_touserdata(L, 1);
    struct timespec *time2 = lua_newuserdata(L, sizeof(*time2));
    time2->tv_sec = -time1->tv_sec;
    time2->tv_nsec = -time1->tv_nsec;
    lua_getmetatable(L, 1);
    lua_setmetatable(L, -2);
    return 1;
}

static void rectify_timestamp(struct timespec *time)
{
    long sec = time->tv_nsec / 1000000000;
    time->tv_nsec -= sec * 1000000000;
    time->tv_sec += sec;
    if (time->tv_sec > 0) {
	if (time->tv_nsec < 0) {
	    time->tv_sec--;
	    time->tv_nsec += 1000000000;
	}
    } else if (time->tv_sec < 0 && time->tv_nsec > 0) {
	time->tv_sec++;
	time->tv_nsec -= 1000000000;
    }
}

LUAFN(time_sum)
{
    struct timespec *time1 = lua_touserdata(L, 1);
    struct timespec *time2;
    if (lua_isnumber(L, 2)) {
	time2 = lua_newuserdata(L, sizeof(*time2));
	add_time_delta(time2, time1, lua_tonumber(L, 2));
	lua_getmetatable(L, 1);
	lua_setmetatable(L, -2);
	return 1;
    }
    if (lua_isnumber(L, 1)) {
	time2 = lua_newuserdata(L, sizeof(*time2));
	add_time_delta(time2, lua_touserdata(L, 2), lua_tonumber(L, 1));
	lua_getmetatable(L, 2);
	lua_setmetatable(L, -2);
	return 1;
    }
    lua_getmetatable(L, 1);
    if (!lua_getmetatable(L, 2) || !lua_rawequal(L, -1, -2))
	luaL_error(L, badtime);
    time2 = lua_touserdata(L, 2);
    struct timespec *timesum = lua_newuserdata(L, sizeof(*timesum));
    timesum->tv_sec = time2->tv_sec + time1->tv_sec;
    timesum->tv_nsec = time2->tv_nsec + time1->tv_nsec;
    rectify_timestamp(timesum);
    lua_getmetatable(L, 1);
    lua_setmetatable(L, -2);
    return 1;
}

LUAFN(create_timestamp)
{
    double arg1 = luaL_checknumber(L, 1);
    bool none = lua_isnone(L, 2);
    struct timespec *time = lua_newuserdata(L, sizeof(*time));
    time->tv_sec = arg1;
    if (none)
	time->tv_nsec = 1E9L*(arg1 - time->tv_sec);
    else {
	time->tv_nsec = luaL_checkinteger(L, 2);
	rectify_timestamp(time);
    }
    lua_pushvalue(L, lua_upvalueindex(1));
    lua_setmetatable(L, -2);
    return 1;
}

LUAFN(timestamp_parts)
{
    if (!lua_getmetatable(L, 1) || !lua_rawequal(L, -1, -1))
	luaL_error(L, badtime);
    struct timespec *time = lua_touserdata(L, 1);
    lua_pushnumber(L, time->tv_sec);
    lua_pushnumber(L, time->tv_nsec);
    return 2;
}

LUAFN(time_scale)
{
    int stampix = lua_isnumber(L, 1) ? 2 : 1;
    long scale_factor = luaL_checkinteger(L, 3-stampix);
    struct timespec *time1 = lua_touserdata(L, stampix);
    struct timespec *time2 = lua_newuserdata(L, sizeof(*time2));
    time2->tv_sec = scale_factor * time1->tv_sec;
    time2->tv_nsec = (scale_factor * time1->tv_nsec);
    time2->tv_nsec %= 1000000000;
    time2->tv_sec += (scale_factor * time1->tv_nsec)/1000000000;
    lua_getmetatable(L, stampix);
    lua_setmetatable(L, -2);
    return 1;
}

LUAFN(time_as_double)
{
    struct timespec *time = lua_touserdata(L, 1);
    lua_pushnumber(L, time->tv_sec + 1E-9L*time->tv_nsec);
    return 1;
}

/************/
/* Messages */
/************/

LUAFN(get_message)
{
    if (!initialized)
	return 0;
    int rc = getmsg(L);
    if (rc > 0)
	sem_wait(&tasks[my_index].incoming_sem);
    return rc;
}

LUAFN(wait_message)
{
    ASSURE_INITIALIZED;
    if (lua_isnoneornil(L, 1))
	sem_wait(&tasks[my_index].incoming_sem);
    else {
	struct timespec time, *sleep_until = &time;
	if (lua_isnumber(L, 1)) {
	    clock_gettime(CLOCK_REALTIME, &time);
	    add_time_delta(&time, &time, lua_tonumber(L, 1));
	} else if (lua_getmetatable(L, 1) &&
		   lua_rawequal(L, -1, lua_upvalueindex(1)))
	    sleep_until = lua_touserdata(L, -2);
	else
	    luaL_error(L, badtime);

	if (sem_timedwait(&tasks[my_index].incoming_sem, sleep_until) < 0)
	    return 0;
	lua_settop(L, 0);
    }

    int rc = getmsg(L);
    return rc;
}

LUAFN(send_message)
{
    ASSURE_INITIALIZED;
    int task_ix = validate_task(L, 2);
    if (task_ix < 0)
	return nosuchtask(L);
    size_t msglen;
    const char *msg =lua_tolstring(L, 1, &msglen);
    if (send_client_msg(&tasks[task_ix], NORMAL_CLIENT_MSG,
			msg, msglen) < 0) {
	lua_pushnil(L);
	lua_pushstring(L, "Queue full");
	return 2;
    }
    lua_pushinteger(L, task_ix);
    return 1;
}

LUAFN(send_message_with_type)
{
    ASSURE_INITIALIZED;
    int task_ix = validate_task(L, 3);
    if (task_ix < 0)
	return nosuchtask(L);
    size_t msglen;
    const char *msg =lua_tolstring(L, 1, &msglen);
    int type = lua_tointeger(L, 2);
    if (type > MAX_MSG_TYPE || type < 0)
	luaL_error(L, "Bad user message type");
    if (send_client_msg(&tasks[task_ix], type, msg, msglen) < 0) {
	lua_pushnil(L);
	lua_pushstring(L, "Queue full");
	return 2;
    } else
	lua_pushinteger(L, task_ix);
    return 1;
}

LUAFN(broadcast_message)
{
    ASSURE_INITIALIZED;
    size_t msglen;
    unsigned int send_count = 0;
    const char *msg =lua_tolstring(L, 1, &msglen);
    int type = lua_tointeger(L, 2);
    if (type > MAX_MSG_TYPE || type < 0)
	luaL_error(L, "Bad broadcast message type");
    int channels = lua_tointeger(L, 3);
    if (channels > CHANNEL_MASK || channels < 1)
	luaL_error(L, "Bad broadcast channel selection");
    for (int i = 0; i < num_tasks; i++) {
	if (i != my_index &&
	    tasks[i].nonce != 0 &&
	    tasks[i].subscriptions & channels)
	    if (send_client_msg(&tasks[i], type | 0x1000 , msg, msglen) >=0)
		send_count++;
    }
    lua_pushinteger(L, send_count);
    return 1;
}

LUAFN(set_reception)
{
    ASSURE_INITIALIZED;
    int mask = lua_tointeger(L, 1);
    if (mask < 0 || mask > CHANNEL_MASK)
	luaL_error(L, "Bad broadcast mask");
    tasks[my_index].subscriptions &= ~CHANNEL_MASK;
    tasks[my_index].subscriptions |= mask;
    return 0;
}

LUAFN(set_subscriptions)
{
    ASSURE_INITIALIZED;
    luaL_checktype(L, 1, LUA_TTABLE);
    int flags = 0;
    for (int i=0; announcements[i].name; i++) {
	lua_getfield(L, 1, announcements[i].name);
	flags |=  (lua_isnil(L, -1) ? 0 : announcements[i].flags);
	lua_pop(L, 1);
    }
    tasks[my_index].subscriptions &= CHANNEL_MASK;
    tasks[my_index].subscriptions += flags;
    return 0;
}

/************************/
/* Scheduler parameters */
/************************/

LUAFN(set_priority)
{
#ifdef __linux__
    ASSURE_INITIALIZED;
    struct sched_param param;

    param.sched_priority = lua_tointeger(L, 1);
    int policy = SCHED_OTHER;
    if (!lua_isnil(L, 2))
	policy = lua_tointeger(L, 2);
    lua_pushboolean(L, pthread_setschedparam(tasks[my_index].thread,
					     policy, &param) == 0);
#else
    lua_pushboolean(L, false);
#endif
    return 1;
}

LUAFN(set_affinity)
{
#ifdef __linux__
    ASSURE_INITIALIZED;
    luaL_checktype(L, 1, LUA_TTABLE);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    int cpucount = sysconf(_SC_NPROCESSORS_CONF);
    int i = 1;
    while (true) {
	lua_rawgeti(L, 1, i++);
	if (lua_isnil(L, -1)) break;
	int cpu = luaL_checkinteger(L ,-1);
	if (cpu < 0 || cpu >= cpucount)
	    luaL_error(L, "Bad cpu number %d", cpu);
	lua_pop(L, 1);
	CPU_SET(cpu, &cpuset);
    }
    lua_pushboolean(L, sched_setaffinity(0, sizeof(cpuset), &cpuset) == 0);
#else
    lua_pushboolean(L, false);
#endif
    return 1;
}

/******************/
/* Status display */
/******************/

LUAFN(status)
{
    if (!initialized)
	fprintf(stderr, "Inactive\n");
    else if (my_index == 0)
	send_ctl_msg(SHOW_STATUS, lua_toboolean(L,1) ? "\1" : "\0", 1);
    return 0;
}

LUALIB_API int luaopen_taskman(lua_State *L)
{
    /********************/
    /* NORMAL FUNCTIONS */
    /********************/

    const luaL_Reg funcptrs[] = {
	FN_ENTRY(broadcast_message),
	FN_ENTRY(broadcast_private_flag),
	FN_ENTRY(cancel_all),
	FN_ENTRY(change_global_flag),
	FN_ENTRY(private_flag),
	FN_ENTRY(get_private_flag_word),
	FN_ENTRY(initialize),
	FN_ENTRY(interrupt_all),
	FN_ENTRY(get_message),
	FN_ENTRY(get_my_name),
	FN_ENTRY(global_flag),
	FN_ENTRY(get_global_flag_word),
	FN_ENTRY(set_affinity),
	FN_ENTRY(set_cancel),
	FN_ENTRY(set_immunity),
	FN_ENTRY(set_priority), // Requires special privs.
	FN_ENTRY(set_reception),
	FN_ENTRY(set_thread_name),
	FN_ENTRY(set_subscriptions),
	FN_ENTRY(shutdown),
	FN_ENTRY(status),
	{ NULL, NULL }
    };

    luaL_register(L, "taskman", funcptrs);

    /************/
    /* CLOSURES */
    /************/

#define STORECCLOSURE(NAME,PARAMS,WHERE)			\
    lua_pushcclosure(L, LUAFN_NAME(NAME), PARAMS);	\
    lua_setfield(L, WHERE, #NAME)

    // Add access to LuaJIT dumper as it has an extra parameter.
    lua_getglobal(L, "string");
    lua_getfield(L, -1, "dump");
    lua_remove(L, -2);
    STORECCLOSURE(create_task, 1, -2);


    // Add metatable for timestamps to sleep(), wait_message(), now(),
    // and create_timestamp().
    lua_newtable(L);
    lua_pushstring(L, "Private");
    lua_setfield(L, -2, "__metatable");
    lua_pushcfunction(L, LUAFN_NAME(timestamp_name));
    lua_setfield(L, -2, "__tostring");
    lua_pushcfunction(L, LUAFN_NAME(time_sum));
    lua_setfield(L, -2, "__add");
    lua_pushcfunction(L, LUAFN_NAME(time_difference));
    lua_setfield(L, -2, "__sub");
    lua_pushcfunction(L, LUAFN_NAME(time_negate));
    lua_setfield(L, -2, "__unm");
    lua_pushcfunction(L, LUAFN_NAME(time_scale));
    lua_setfield(L, -2, "__mul");
    lua_pushcfunction(L, LUAFN_NAME(time_as_double));
    lua_setfield(L, -2, "__len");
    lua_pushvalue(L, -1);
    STORECCLOSURE(create_timestamp, 1, -3);
    lua_pushvalue(L, -1);
    STORECCLOSURE(timestamp_parts, 1, -3);
    lua_pushvalue(L, -1);
    STORECCLOSURE(now, 1, -3);
    lua_pushvalue(L, -1);
    STORECCLOSURE(sleep, 1, -3);
    STORECCLOSURE(wait_message, 1, -2);

    // Users of validate_task have the task name -> index cache
    // as an upvalue.
    lua_newtable(L);
    lua_pushvalue(L, -1);
    STORECCLOSURE(cancel_task, 1, -3);
    lua_pushvalue(L, -1);
    STORECCLOSURE(change_private_flag, 1, -3);
    lua_pushvalue(L, -1);
    STORECCLOSURE(interrupt_task, 1, -3);
    lua_pushvalue(L, -1);
    STORECCLOSURE(lookup_task, 1, -3);
    lua_pushvalue(L, -1);
    STORECCLOSURE(send_message, 1, -3);
    STORECCLOSURE(send_message_with_type, 1, -2);

    /*************/
    /* CONSTANTS */
    /*************/

    // Add task announcements.
    lua_pushinteger(L, TASK_BAD_CREATE);
    lua_setfield(L, -2, "task_create_failed");
    lua_pushinteger(L, TASK_CREATE);
    lua_setfield(L, -2, "task_created");
    lua_pushinteger(L, TASK_EXIT);
    lua_setfield(L, -2, "task_exited");
    lua_pushinteger(L, TASK_FAILURE);
    lua_setfield(L, -2, "task_failed");
    lua_pushinteger(L, TASK_CANCEL);
    lua_setfield(L, -2, "task_cancelled");

#ifdef __linux__
    // Add schuduler types.
    lua_pushinteger(L, SCHED_RR);
    lua_setfield(L, -2, "sched_rr");
    lua_pushinteger(L, SCHED_FIFO);
    lua_setfield(L, -2, "sched_fifo");
    lua_pushinteger(L, SCHED_IDLE);
    lua_setfield(L, -2, "sched_idle");
    lua_pushinteger(L, SCHED_BATCH);
    lua_setfield(L, -2, "sched_batch");
    lua_pushinteger(L, SCHED_OTHER);
    lua_setfield(L, -2, "sched_other");
#endif

    return 1;
}
