/**************************************************************************/
/*                                                                        */
/*      Copyright (c) 2005-2019 by ITTIA L.L.C. All rights reserved.      */
/*                                                                        */
/*  This software is copyrighted by and is the sole property of ITTIA     */
/*  L.L.C.  All rights, title, ownership, or other interests in the       */
/*  software remain the property of ITTIA L.L.C.  This software may only  */
/*  be used in accordance with the corresponding license agreement.  Any  */
/*  unauthorized use, duplication, transmission, distribution, or         */
/*  disclosure of this software is expressly forbidden.                   */
/*                                                                        */
/*  This Copyright notice may not be removed or modified without prior    */
/*  written consent of ITTIA L.L.C.                                       */
/*                                                                        */
/*  ITTIA L.L.C. reserves the right to modify this software without       */
/*  notice.                                                               */
/*                                                                        */
/*  info@ittia.com                                                        */
/*  http://www.ittia.com                                                  */
/*                                                                        */
/*                                                                        */
/**************************************************************************/

#ifndef EXAMPLE_THREAD_H
#define EXAMPLE_THREAD_H

#include <ittia/os/os_thread.h>
#include <ittia/os/os_wait_time.h>

/// Thread class for ITTIA C++ examples.
class example_thread
{
public:
    typedef void (*thread_proc_t)(void*);

    /// Call the function @a task with argument @a arg in a background thread.
    example_thread(thread_proc_t task, void * arg);

    /// Wait for the background thread to finish.
    void join();

public:
    /// Sleep in the current thread for the given number of seconds
    static void sleep_for_seconds(int32_t seconds);

    /// Sleep in the current thread for the given number of milliseconds
    static void sleep_for_milliseconds(int32_t milliseconds);

private:
    os_thread_t * thread;
};


inline example_thread::example_thread(thread_proc_t task, void * arg)
{
    (void)os_thread_spawn(task,  arg, DEFAULT_STACK_SIZE, OS_THREAD_JOINABLE, &thread);
}

inline void example_thread::join()
{
    (void)os_thread_join(thread);
}

inline void example_thread::sleep_for_seconds(int32_t seconds)
{
    (void)os_sleep(WAIT_MILLISEC(seconds * 1000));
}

inline void example_thread::sleep_for_milliseconds(int32_t milliseconds)
{
    (void)os_sleep(WAIT_MILLISEC(milliseconds));
}

#endif
