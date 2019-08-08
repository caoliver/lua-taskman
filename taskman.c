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

#include "lua_head.h"

#if 0
// Leave this here for feature debugging
void show_stack(lua_State *L)
{
    printf("TOP IS %d\n", lua_gettop(L));
    for (int i = lua_gettop(L); i > 0; i--)
	printf("\t%d:\t%s\n", i, lua_typename(L, lua_type(L, i)));
}
#endif

#include "mmaputil.h"

#define MIN_TASKS 64
// This is a uint16_t.  Maybe make this smaller.
#define MAX_TASKS 65536
#define MIN_CONTROL_BUFFER_SIZE (1<<18)
//#define MIN_CLIENT_BUFFER_SIZE (1<<18)
#define MIN_CLIENT_BUFFER_SIZE (1<<18)

// Wake 50 times per second.
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
#define TASK_CREATE  4096
#define TASK_EXIT 4097
#define TASK_FAILURE 4098
#define TASK_CANCEL   4099

#define MAX_FLAG 11
#define MAX_MSG_TYPE 4095
#define BROADCAST_SHIFT 12
#define CHANNEL_COUNT 12

#define SUBSCRIBE_SHIFT 24
char *announcements[] = {
#define ANNOUNCE_CHILD_EXITS (1<<SUBSCRIBE_SHIFT)
    "child_task_exits",
#define ANNOUNCE_ALL_TASK_EXITS (2<<SUBSCRIBE_SHIFT)
    "any_task_exits",
#define ANNOUNCE_NEW_NAMED_TASKS (4<<SUBSCRIBE_SHIFT)
    "new_named_task",
#define ANNOUNCE_ANY_NEW_TASKS (8<<SUBSCRIBE_SHIFT)
    "any_new_task",
    NULL};


// Align message lengths on double words.
#define ALIGN(N) ((N)+7 & ~7)

__thread lua_State *L;

__thread uint16_t my_index;

static char *tasklist_name = "__task_list";
static char *housekeeper_name = "HouseKeeper";
static char *maintask_name = ":main:";

static sem_t task_running_sem;

struct task {
    uint16_t nonce, last_active_nonce;
    uint16_t parent_index;
    uint16_t parent_nonce;
    uint32_t control_flags;
    uint32_t queue_in_use;
    struct circbuf incoming_queue;
    uint8_t *incoming_store;
    pthread_t thread;
    sem_t housekeeper_pending;
    sem_t incoming_sem;
    pthread_mutex_t incoming_mutex;
    int query_index;
    int query_extra;
    uint32_t name_in_use;
    char *name;
    lua_State *my_state;
};

int num_tasks;
struct task *tasks;

pthread_t housekeeper_thread;

static char *bad_msg = "%s: Bad message";

static size_t control_channel_size;
static uint8_t *control_channel_store;
static struct circbuf control_channel_buf;
static sem_t control_channel_sem;
static pthread_mutex_t control_channel_mutex;

struct message {
    uint32_t size;
    uint16_t type;
    uint16_t sender;
    uint16_t nonce;
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
    pthread_cancel(tasks[my_index].thread);
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
	msg->nonce = tasks[my_index].nonce;
	msg->type = type;
	msg->size = size;
	memcpy(msg->payload, payload, size);
	cb_produce(&task->incoming_queue, msgsize);
	success = true;
    }
    if (__atomic_sub_fetch(&task->queue_in_use, 1, __ATOMIC_SEQ_CST) == 0 &&
	tasks->incoming_store) {
	free_twinmap(task->incoming_store, task->incoming_queue.size);
	tasks->incoming_store = 0;
    }
    pthread_mutex_unlock(&task->incoming_mutex);
    if (!success) return -1;
    sem_post(&task->incoming_sem);
    return 0;
}

static int wait_for_reply(lua_State *L)
{
    struct task *mytask = &tasks[my_index];

    sem_wait(&mytask->housekeeper_pending);
    lua_pushinteger(L, mytask->query_index);
    lua_pushinteger(L, mytask->query_extra);
    return 2;
}

// These need to be shared between new_thread and the cancellation handler.
static __thread bool show_errors;
static __thread bool show_exits;
static __thread struct task *task;  // Will be shadowed later on.

void cancellation_handler(void *dummy)
{
    static char cancelmsg[] = "cancelled!";

    if (show_exits || show_errors)
	if (task->name)
	    fprintf(stderr, "Task %s has been cancelled\n", task->name);
	else
	    fprintf(stderr, "Task %d,%d has been cancelled\n",
		    my_index, task->nonce);

    for (int i=0; i < num_tasks; i++)
	if (tasks[i].nonce != 0 &&
	    (i==task->parent_index && tasks[i].nonce == task->parent_nonce &&
	     tasks[i].control_flags & ANNOUNCE_CHILD_EXITS ||
	     tasks[i].control_flags & ANNOUNCE_ALL_TASK_EXITS))
	    send_client_msg(&tasks[i], TASK_CANCEL, cancelmsg,
			    sizeof(cancelmsg));

    tasks[my_index].nonce = 0;
    if (send_ctl_msg(THREAD_EXITS, "C", 1))
	errx(1, "Can't send task exit message.");
    lua_close(L);    // Possibly crashy.  Leaks otherwise.
}

static void *new_thread(void *luastate)
{
    // Race?
    int previous_cancel;
    bool succeeded = false;
    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &previous_cancel);
    sem_post(&task_running_sem);
    L = luastate;
    // Stack:
    //   3 - my index
    //   2 - program description table
    //   1 - Traceback function
    my_index = lua_tointeger(L, -1);
    task = &tasks[my_index];
    lua_pop(L, 1);

    // These aren't GCable.
    __atomic_add_fetch(&task->queue_in_use, 1, __ATOMIC_SEQ_CST);
    __atomic_add_fetch(&task->name_in_use, 1, __ATOMIC_SEQ_CST);

    // Set exit status printing as asked.
    lua_getfield(L, -1, "show_errors");
    show_errors = lua_toboolean(L, -1);
    lua_pop(L, 1);
    lua_getfield(L, -1, "show_exits");
    show_exits = lua_toboolean(L, -1);
    lua_pop(L, 1);

    // Fetch the program.
    lua_getfield(L, -1, "program");
    int rc = 0;
    if (lua_type(L, -1) == LUA_TSTRING &&
	lua_objlen(L, -1) > 1) {
	const char *progtext=lua_tostring(L, -1);
	if (progtext[0] == ':')
	    rc = luaL_loadfile(L, &progtext[1]);
	else
	    rc = luaL_loadstring(L, progtext);
	lua_remove(L, -2);
    }
    // If we don't have a function now, then caller made a booboo.
    if (!lua_isfunction(L, -1)) {
	if (rc == 0)
	    lua_pushfstring(L, "Invalid program type: %s",
			    lua_typename(L, lua_type(L, -1)));
	goto bugout;
    }

    // Setup arguments from program description.
    int arg_count = 0;
    while (1) {
	lua_rawgeti(L, 2, ++arg_count);
	if (lua_isnil(L, -1)) {
	    lua_pop(L, 1);
	    arg_count--;
	    break;
	}
    }
    // We're done with the program description now.
    lua_remove(L, 2);
    if (++task->last_active_nonce == 0)
	// Nonce zero means unused, so skip over it.
	task->last_active_nonce = 1;
    task->nonce = task->last_active_nonce;

    for (int i=0; i < num_tasks; i++)
	if (tasks[i].nonce != 0 &&
	    (tasks[i].control_flags & ANNOUNCE_NEW_NAMED_TASKS &&
	     tasks[my_index].name ||
	     tasks[i].control_flags & ANNOUNCE_ANY_NEW_TASKS))
	    send_client_msg(&tasks[i], TASK_CREATE, "", 0);

    int pcall_succeeds = 0;
    pthread_cleanup_push(cancellation_handler, NULL);
    pthread_setcancelstate(previous_cancel, NULL);
    pcall_succeeds = lua_pcall(L, arg_count, LUA_MULTRET, 1);
    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
    pthread_cleanup_pop(0);

    // Call user program
    if (!pcall_succeeds) {
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
	succeeded = !lua_pcall(L, 1, 1, 1);
    }

bugout:
    if (succeeded && show_exits)
	if (task->name)
	    fprintf(stderr, "Task %s exits.\n", task->name);
	else
	    fprintf(stderr, "Task %d,%d exits.\n", my_index, task->nonce);

    if (!succeeded && (show_errors || show_exits))
	if (task->name)
	    fprintf(stderr, "Task %s failed: %s\n",
		    task->name, lua_tostring(L, -1));
	else
	    fprintf(stderr, "Task %d,%d failed: %s\n",
		    my_index, task->nonce, lua_tostring(L, -1));

    for (int i=0; i < num_tasks; i++)
	if (tasks[i].nonce != 0 &&
	    (i==task->parent_index && tasks[i].nonce == task->parent_nonce &&
	     tasks[i].control_flags & ANNOUNCE_CHILD_EXITS ||
	     tasks[i].control_flags & ANNOUNCE_ALL_TASK_EXITS))
		send_client_msg(&tasks[i],
				succeeded ? TASK_EXIT : TASK_FAILURE,
				lua_tostring(L, -1), lua_objlen(L, -1));

    tasks[my_index].nonce = 0;
    if (send_ctl_msg(THREAD_EXITS, succeeded ? "S" : "F", 1))
	errx(1, "Can't send task exit message.");
    lua_close(L);
    return NULL;
}

static int create_task(uint8_t *taskdescr, int size,
		       uint16_t sender, uint16_t sender_nonce)
{
    lua_State *newstate;
    struct task *task = NULL;
    static unsigned int last_task_allocated;
    // Create tast is invoked from housekeeper only, so L refers to
    // housekeeper's dictionary store rather than client's lua_States.

    int freetask = -1;
    // Look for a free slot O(n).
    int next_task_allocated = last_task_allocated;
    for (int i = 1; i < num_tasks; i++) {
	if (++next_task_allocated == num_tasks)
	    next_task_allocated = 1;
	// Previously used task is available if child has cleared
	// the nonce, and housekeeper has freed the queue storage.
	if (tasks[next_task_allocated].nonce == 0 &&
	    tasks[next_task_allocated].incoming_store == 0) {
	    freetask = next_task_allocated;
	    break;
	}
    }
    newstate = luaL_newstate();
    if (freetask < 0) {
	lua_pushstring(newstate, "No tasks available.");
	goto bugout;
    }
    luaL_openlibs(newstate);
    // Add task ID cache
    lua_newtable(newstate);
    lua_setglobal(newstate, tasklist_name);
    // Add error handler
    lua_pushcfunction(newstate, LUAFN_NAME(traceback));
    // Unpack task description
    lua_pushcfunction(newstate, freezer_thaw_buffer);
    lua_pushlightuserdata(newstate, (void *)taskdescr);
    lua_pushinteger(newstate, size);
    // This shouldn't happen unless freezer is broken.
    if (lua_pcall(newstate, 2,1,-4)) {
	lua_pushfstring(newstate, "Freezer decode failed: %s",
			lua_tostring(newstate, -1));
	lua_remove(newstate, -2);
	goto bugout;
    }
    task = &tasks[freetask];
    // pthread_cancel needs this for cleanup.
    task->my_state = newstate;
    task->control_flags = 0;

    // Set task name if given one.
    lua_getfield(newstate, -1, "task_name");
    if (lua_isnil(newstate, -1)) {
	task->name = NULL;
    } else {
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
	task->name = strdup(namestr);
    }
    lua_pop(newstate, 1);

    //  Create incoming message queue.
    lua_getfield(newstate, -1, "queue_size");
    int queue_size = lua_tointeger(newstate, -1);
    if (!lua_isnil(newstate, -1) && !lua_isnumber(newstate, -1)) {
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
    // Get the function, file, or string chunk.
    lua_pushinteger(newstate, freetask);
    if (pthread_create(&task->thread, NULL, new_thread,
		       (void *)newstate) < 0) {
	lua_pushstring(newstate, "Can't create thread");
	goto bugout;
    }
    last_task_allocated = next_task_allocated;
    // Save task in dictionary.
    if (task->name) {
	lua_pushstring(L, task->name);
	lua_pushinteger(L, freetask);
	lua_rawset(L, 1);
    }

    return freetask;
bugout:
    puts(lua_tostring(newstate, -1));
    if (task) {
	if (task->incoming_store)
	    free_twinmap(task->incoming_store, task->incoming_queue.size);
	if (task->name)
	    free(task->name);
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
		int child_ix = create_task(msg->payload, msg->size,
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
	    printf("\nRunning: %d / Finished: %d / "
		   "Failed: %d / Cancelled: %d\n\n",
		   task_count+1, finished, failed, cancelled);
	    int anon = 0;
	    for (int i = 0; i < num_tasks; i++) {
	    	if (tasks[i].nonce != 0) {
		    if (tasks[i].name)
			fprintf(stderr, "     %d, %s\n", i, tasks[i].name);
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
	    tasks[sender].control_flags=0;
	    pthread_join(tasks[sender].thread, NULL);
	    if (--task_count <= 0 && shutdown)
		goto fini;
	    if (__atomic_sub_fetch(&tasks[sender].name_in_use, 1,
				       __ATOMIC_SEQ_CST) == 0 &&
		tasks[sender].name) {
		lua_pushnil(L);
		lua_setfield(L, 1, tasks[sender].name);
		free(tasks[sender].name);
		tasks[sender].name = NULL;
	    }
	    // Toss message queue store if GCable.
	    if (__atomic_sub_fetch(&tasks[sender].queue_in_use, 1,
				   __ATOMIC_SEQ_CST) == 0 &&
		tasks->incoming_store) {
		free_twinmap(tasks[sender].incoming_store,
			     tasks[sender].incoming_queue.size);
		tasks[sender].incoming_store = 0;
	    }
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
	lua_settop(L, 2);
    }

fini:
    sem_post(&tasks[0].housekeeper_pending);
    lua_close(L);
    signal(SIGUSR1, SIG_DFL);
    signal(SIGUSR2, SIG_DFL);
    return NULL;
}

#define ASSURE_INITIALIZED if (!initialized) initialize(L, 0, 0, 0)
#define TASK_FAIL_IF_UNINITIALISED \
    do if (!initialized) { lua_pushinteger(L, -1); return 1; } while(0)

static int initialize(lua_State *L,
		      unsigned int task_limit,
		      size_t control_channel_size,
		      size_t main_incoming_channel_size)
{
    if (initialized)
	luaL_error(L, "TASKMAN is already initialized");
    initialized = true;

    lua_newtable(L);
    lua_setglobal(L, tasklist_name);

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
    tasks->name = maintask_name;
    tasks->nonce = 1;
    tasks->queue_in_use = 1;
    tasks->name_in_use = 1;
    sem_init(&tasks->housekeeper_pending, 0, 0);

    return pthread_create(&housekeeper_thread, NULL, &housekeeper, NULL);
}

LUAFN(initialize)
{
    unsigned int task_limit = 0;
    size_t control_channel_size = 0;
    size_t main_incoming_channel_size = 0;

    if (lua_istable(L, 1)) {
	lua_settop(L, 1);
	lua_getfield(L, 1, "task_limit");
	task_limit = lua_tonumber(L, -1);
	lua_getfield(L, 1, "control_channel_bytes");
	control_channel_size = lua_tonumber(L, -1);
	lua_getfield(L, 1, "main_channel_bytes");
	main_incoming_channel_size = lua_tonumber(L, -1);
	lua_settop(L, 0);
    }

    lua_pushboolean(L, initialize(L,
				  task_limit,
				  control_channel_size,
				  main_incoming_channel_size));
    return 0;
}

LUAFN(create_task)
{
    ASSURE_INITIALIZED;
    lua_settop(L, 1);
    luaL_checktype(L, 1, LUA_TTABLE);
    freezer_freeze(L);
    size_t size = lua_objlen(L, -1);
    if (send_ctl_msg(CREATE_TASK, lua_tostring(L, -1), size))
	return 0;
    return wait_for_reply(L);
}

struct task_cache_entry {
    uint16_t task_ix;
    uint16_t nonce;
};

int lookup_task(lua_State *L, const char *name)
{
    int top = lua_gettop(L);
    lua_getglobal(L, tasklist_name);
    lua_getfield(L, -1, name);
    // Name / table / lookup
    if (!lua_isnil(L, -1)) {
	struct task_cache_entry *task_entry = (void *)lua_tostring(L, -1);
	if (tasks[task_entry->task_ix].nonce == task_entry->nonce)
	    return task_entry->task_ix;
    }
    lua_pop(L, 1);
    if (send_ctl_msg(GET_TASK_INDEX, name, strlen(name))) {
	lua_settop(L, top);
	return -2;
    }
    int result = wait_for_reply(L);
    if (result > 0) {
	if ((result = lua_tointeger(L, -2)) >= 0) {
	    struct task_cache_entry task_entry;
	    task_entry.task_ix = result;
	    task_entry.nonce = lua_tointeger(L, -1);
	    lua_pushlstring(L, (void *)&task_entry, sizeof(task_entry));
	} else
	    lua_pushnil(L);
	lua_setfield(L, -4, name);
    }
    lua_settop(L, top);
    return result;
}

int validate_task(lua_State *L, int ix)
{
    int task_ix;
    if (lua_isnumber(L, ix)) {
	task_ix = luaL_checkinteger(L, ix);
	if (task_ix < 0 || task_ix >= num_tasks)
	    return -1;
	if (lua_isnoneornil(L, ix+1)) {
	    if (tasks[task_ix].nonce == 0)
		return -1;
	} else if (luaL_checkinteger(L, ix+1) != tasks[task_ix].nonce)
	    return -1;
    } else
	task_ix = lookup_task(L, luaL_checkstring(L, ix));
    return task_ix;
}

LUAFN(lookup_task)
{
    TASK_FAIL_IF_UNINITIALISED;
    int task_ix = validate_task(L, 1);
    if (task_ix < 0)
	return 0;
    lua_pushinteger(L, task_ix);
    lua_pushinteger(L, tasks[task_ix].nonce);
    return 2;
}

LUAFN(shutdown)
{
    if (initialized && my_index == 0) {
	send_ctl_msg(REQUEST_SHUTDOWN, "", 0);
	sem_wait(&tasks[my_index].housekeeper_pending);
	pthread_join(housekeeper_thread, NULL);
	free(tasks);
	free_twinmap(control_channel_store, control_channel_size);
	initialized = false;
    }
    return 0;
}

LUAFN(status)
{
    if (!initialized)
	puts("Inactive");
    else if (my_index == 0)
	send_ctl_msg(SHOW_STATUS, lua_toboolean(L,1) ? "\1" : "\0", 1);
    return 0;
}

static char *badflag = "Invalid flag %d";

LUAFN(change_flag)
{
    ASSURE_INITIALIZED;
    int flag = luaL_checkinteger(L, 1);
    if (flag < 0 || flag >= MAX_FLAG)
	luaL_error(L, badflag, flag);
    int task_ix = validate_task(L, 3);
    if (task_ix < 0)
	return 0;
     tasks[task_ix].control_flags = (tasks[task_ix].control_flags &
				     ~(1<<flag) |
				     lua_toboolean(L, 2)<<flag);
    lua_pushinteger(L, task_ix);
    return 1;
}

LUAFN(broadcast_flag)
{
    ASSURE_INITIALIZED;
    int flag = luaL_checkinteger(L, 1);
    if (flag < 0 || flag > MAX_FLAG)
	luaL_error(L, badflag, flag);
    for (int i = 0; i < num_tasks; i++)
	if (tasks[i].nonce != 0)
	    tasks[i].control_flags = (tasks[i].control_flags &
				      ~(1<<flag) |
				      lua_toboolean(L, 2)<<flag);

    return 0;
}

LUAFN(flag_is_true)
{
    ASSURE_INITIALIZED;
    int flag = luaL_checkinteger(L, 1);
    if (flag < 0 || flag > MAX_FLAG)
	luaL_error(L, badflag, flag);
    lua_pushboolean(L, tasks[my_index].control_flags & 1<<flag);
    return 1;
}

LUAFN(interrupt_task)
{
    TASK_FAIL_IF_UNINITIALISED;
    int signo = 0;
    if (!lua_isnil(L, 1))
	signo = lua_toboolean(L, 1) ? 12 : 10;
    int task_ix = validate_task(L, 2);
    if (task_ix > 0 && pthread_kill(tasks[task_ix].thread, signo) != 0)
	task_ix = -1;
    lua_pushinteger(L, task_ix);
    return 1;
}

LUAFN(interrupt_all)
{
    TASK_FAIL_IF_UNINITIALISED;
    int signo = 0;
    if (!lua_isnil(L, 1))
	signo = lua_toboolean(L, 1) ? 12 : 10;
    for (int i = 1; i < num_tasks; i++)
	if (tasks[i].nonce != 0 && i != my_index)
	    pthread_kill(tasks[i].thread, signo);
    return 0;
}

LUAFN(cancel_task)
{
    TASK_FAIL_IF_UNINITIALISED;
    int task_ix = validate_task(L, 1);
    if (task_ix > 0)
	pthread_cancel(tasks[task_ix].thread);
    lua_pushinteger(L, task_ix);
    return 1;
}

LUAFN(cancel_all)
{
    TASK_FAIL_IF_UNINITIALISED;
    for (int i = 1; i < num_tasks; i++)
	if (tasks[i].nonce != 0 && i != my_index)
	    pthread_cancel(tasks[i].thread);
    return 0;
}

LUAFN(set_cancel)
{
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
				  PTHREAD_CANCEL_ASYNCHRONOUS
				  : PTHREAD_CANCEL_DEFERRED, &oldtype);
    }
    lua_pushinteger(L, oldstate);
    lua_pushinteger(L, oldtype);
    return 2;
}

static int getmsg(lua_State *L)
{
    struct task *task = &tasks[my_index];
    struct message *msg;

    if (cb_occupied(&task->incoming_queue) == 0)
	return 0;
    msg = (void *)(cb_head(&task->incoming_queue) + task->incoming_store);
    uint32_t msgmax = cb_occupied(&task->incoming_queue);
    if (msgmax < sizeof(struct message)) {
	errx(1, bad_msg, "client");
    }
    int sender = msg->sender;
    struct task *sender_task = &tasks[sender];
    size_t size = msg->size;
    lua_pushlstring(L, (char *)msg->payload, size);
    lua_pushinteger(L, msg->type);
    cb_release(&task->incoming_queue, ALIGN(sizeof(struct message)+size));
    int retcnt = 4;
    if (__atomic_add_fetch(&sender_task->name_in_use, 1,
			   __ATOMIC_SEQ_CST) == 2 &&
	sender_task->name) {
	lua_pushstring(L, sender_task->name);
	retcnt++;
    }
    if (__atomic_sub_fetch(&sender_task->name_in_use, 1,
			   __ATOMIC_SEQ_CST) == 0 &&
	sender_task->name) {
	free(task->name);
	task->name = NULL;
    }
    lua_pushinteger(L, sender);
    lua_pushinteger(L, sender_task->nonce);
    return retcnt;
}

LUAFN(get_message)
{
    ASSURE_INITIALIZED;
    int rc = getmsg(L);
    if (rc > 0)
	sem_wait(&tasks[my_index].incoming_sem);
    return rc;
}

LUAFN(timestamp_name)
{
    lua_pushfstring(L, "timestamp (%p)", lua_topointer(L, 1));
    return 1;
}

LUAFN(time_from_now)
{
    double interval = lua_tonumber(L, 1);
    if (interval < 0)
	interval = 0;
    struct timespec *ts;
    ts = lua_newuserdata(L, sizeof(*ts));
    clock_gettime(CLOCK_REALTIME, ts);
    interval += ts->tv_sec + (double)1E-9*ts->tv_nsec;
    ts->tv_sec = interval;
    ts->tv_nsec = 1E9*(interval - ts->tv_sec);
    lua_pushvalue(L, lua_upvalueindex(1));
    lua_setmetatable(L, -2);
    lua_replace(L, 1);
    return 1;
}

LUAFN(wait_message)
{
    ASSURE_INITIALIZED;
    if (lua_isnone(L, 1))
	sem_wait(&tasks[my_index].incoming_sem);
    else {
	if (lua_isnumber(L, 1))
	    CALL_LUAFN(time_from_now);
	if (!lua_getmetatable(L, 1) ||
	    !lua_rawequal(L, -1, lua_upvalueindex(1)))
	    luaL_error(L, "Bad time spec");
	struct timespec *ts = lua_touserdata(L, -2);
	if (sem_timedwait(&tasks[my_index].incoming_sem, ts) < 0)
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
    if (task_ix >= 0) {
	size_t msglen;
	const char *msg =lua_tolstring(L, 1, &msglen);
	if (send_client_msg(&tasks[task_ix], NORMAL_CLIENT_MSG,
			    msg, msglen) < 0)
	    // No room, so set error code.
	    task_ix=-2;
    }
    lua_pushinteger(L, task_ix);
    return 1;
}

LUAFN(send_message_with_type)
{
    ASSURE_INITIALIZED;
    int task_ix = validate_task(L, 3);
    if (task_ix >= 0) {
	size_t msglen;
	const char *msg =lua_tolstring(L, 1, &msglen);
	int type = lua_tointeger(L, 2);
	if (type > MAX_MSG_TYPE || type < 0)
	    luaL_error(L, "Bad user message type");
	if (send_client_msg(&tasks[task_ix], type, msg, msglen) < 0)
	    // No room, so set error code.
	    task_ix=-2;
    }
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
    if (channels > (1<<CHANNEL_COUNT) - 1 || channels < 1)
	luaL_error(L, "Bad broadcast channel selection");
    for (int i = 0; i < num_tasks; i++) {
	if (i != my_index &&
	    tasks[i].nonce != 0 &&
	    tasks[i].control_flags & channels<<BROADCAST_SHIFT)
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
    if (mask < 0 || mask > 255)
	luaL_error(L, "Bad broadcast mask");
    tasks[my_index].control_flags &= ~(255<<BROADCAST_SHIFT);
    tasks[my_index].control_flags |= mask<<BROADCAST_SHIFT;
    return 0;
}

LUAFN(set_subscriptions)
{
    ASSURE_INITIALIZED;
    luaL_checktype(L, 1, LUA_TTABLE);
    int flags = tasks[my_index].control_flags;
    for (int i=0; announcements[i]; i++) {
	lua_getfield(L, 1, announcements[i]);
	flags = (flags & ~(1<<SUBSCRIBE_SHIFT+i)) |
	    (lua_isnil(L, -1) ? 0 : (1<<SUBSCRIBE_SHIFT+i));
	lua_pop(L, 1);
    }
    tasks[my_index].control_flags = flags;
    return 0;
}

LUAFN(set_priority)
{
#ifdef __linux__
    ASSURE_INITIALIZED;
    struct sched_param param;

    param.sched_priority = lua_tointeger(L, 1);
    int policy = SCHED_OTHER;
    if (!lua_isnil(L, 2))
	policy = lua_tointeger(L, 2);
    lua_pushboolean(L,
		    pthread_setschedparam(tasks[my_index].thread,
					  policy, &param) == 0);
    return 1;
#else
    luaL_error(L, "**NOT IMPLEMENTED**");
#endif
}

LUAFN(set_affinity)
{
#ifdef __linux__
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
    return 1;
#else
    luaL_error(L, "**NOT IMPLEMENTED**");
#endif
}

LUALIB_API int luaopen_taskman(lua_State *L)
{
    const luaL_Reg funcptrs[] = {
	FN_ENTRY(initialize),
	FN_ENTRY(lookup_task),
	FN_ENTRY(change_flag),
	FN_ENTRY(broadcast_flag),
	FN_ENTRY(interrupt_task),
	FN_ENTRY(interrupt_all),
	FN_ENTRY(cancel_task),
	FN_ENTRY(cancel_all),
	FN_ENTRY(set_cancel),
	FN_ENTRY(flag_is_true),
	FN_ENTRY(shutdown),
	FN_ENTRY(send_message),
	FN_ENTRY(send_message_with_type),
	FN_ENTRY(broadcast_message),
	FN_ENTRY(set_reception),
	FN_ENTRY(set_subscriptions),
	FN_ENTRY(get_message),
	FN_ENTRY(status),
	FN_ENTRY(set_priority), // Requires special privs.
	FN_ENTRY(set_affinity),
	{ NULL, NULL }
    };

    luaL_register(L, "taskman", funcptrs);

    lua_pushinteger(L, TASK_EXIT);
    lua_setfield(L, -2, "task_exits");
    lua_pushinteger(L, TASK_FAILURE);
    lua_setfield(L, -2, "task_failure");
    lua_pushinteger(L, TASK_CANCEL);
    lua_setfield(L, -2, "task_cancelled");

    lua_newtable(L);
    lua_pushinteger(L, SCHED_RR);
    lua_setfield(L, -2, "rr");
    lua_pushinteger(L, SCHED_FIFO);
    lua_setfield(L, -2, "fifo");
    lua_pushinteger(L, SCHED_IDLE);
    lua_setfield(L, -2, "idle");
    lua_pushinteger(L, SCHED_BATCH);
    lua_setfield(L, -2, "batch");
    lua_pushinteger(L, SCHED_OTHER);
    lua_setfield(L, -2, "other");
    lua_setfield(L, -2, "sched");

    lua_newtable(L);
    lua_pushinteger(L, TASK_CREATE);
    lua_setfield(L, -2, "created");
    lua_pushinteger(L, TASK_EXIT);
    lua_setfield(L, -2, "exited");
    lua_pushinteger(L, TASK_FAILURE);
    lua_setfield(L, -2, "failed");
    lua_pushinteger(L, TASK_CANCEL);
    lua_setfield(L, -2, "cancelled");
    lua_setfield(L, -2, "announce");

    lua_getglobal(L, "string");
    lua_getfield(L, -1, "dump");
    lua_pushcclosure(L, LUAFN_NAME(create_task), 1);
    lua_remove(L, -2);
    lua_setfield(L, -2, "create_task");

    // Add metatable for timestamps to waitmsg() and from_now().
    lua_newtable(L);
    lua_pushstring(L, "Private");
    lua_setfield(L, -2, "__metatable");
    lua_pushcfunction(L, LUAFN_NAME(timestamp_name));
    lua_setfield(L, -2, "__tostring");
    lua_pushvalue(L, -1);
    lua_pushcclosure(L, LUAFN_NAME(wait_message), 1);
    lua_setfield(L, -3, "wait_message");
    lua_pushcclosure(L, LUAFN_NAME(time_from_now), 1);
    lua_setfield(L, -2, "time_from_now");

    return 1;
}
