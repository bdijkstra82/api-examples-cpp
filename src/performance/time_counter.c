#include "time_counter.h"

#ifdef OS_UCOS_III
#include <os.h>
#define NOT_HAVE_SYS_TIME_H
#endif

#include <string.h>
#include <time.h>

#if defined(_WIN32)
#include <windows.h>

size_t mkms_time()
{
    LARGE_INTEGER li;
    LARGE_INTEGER fq;

    if (!QueryPerformanceCounter(&li)
        || !QueryPerformanceFrequency(&fq))
        return GetTickCount();

    return (size_t)(li.QuadPart * 1000 / fq.QuadPart);
}

void get_elapsed_counters(uint64_t * real_counter, uint64_t * kernel_counter, uint64_t * user_counter)
{
    LARGE_INTEGER li;
    FILETIME creationFileTime, exitFileTime, kernelFileTime, userFileTime;

    if (QueryPerformanceCounter(&li))
        *real_counter = li.QuadPart;
    else
        *real_counter = GetTickCount();

    if (GetThreadTimes(GetCurrentThread(), &creationFileTime, &exitFileTime, &kernelFileTime, &userFileTime)) {
        ULARGE_INTEGER kernelIntTime, userIntTime;
        kernelIntTime.HighPart = kernelFileTime.dwHighDateTime;
        kernelIntTime.LowPart = kernelFileTime.dwLowDateTime;
        userIntTime.HighPart = userFileTime.dwHighDateTime;
        userIntTime.LowPart = userFileTime.dwLowDateTime;

        *kernel_counter = kernelIntTime.QuadPart;
        *user_counter = userIntTime.QuadPart;
    }
    else {
        *kernel_counter = 0;
        *user_counter = 0;
    }
}

void get_elapsed_frequency(uint64_t * real_freq, uint64_t * kernel_freq, uint64_t * user_freq)
{
    LARGE_INTEGER fq;
    FILETIME creationFileTime, exitFileTime, kernelFileTime, userFileTime;

    if (QueryPerformanceFrequency(&fq))
        *real_freq = fq.QuadPart;
    else
        *real_freq = 1000;

    if (GetThreadTimes(GetCurrentThread(), &creationFileTime, &exitFileTime, &kernelFileTime, &userFileTime)) {
        *kernel_freq = 10000;
        *user_freq = 10000;
    }
    else {
        *kernel_freq = 0;
        *user_freq = 0;
    }
}

#elif defined(CPU_CFG_TS_64_EN) && (CPU_CFG_TS_64_EN == DEF_ENABLED)

size_t mkms_time()
{
    OS_ERR err;
    /* Tick rate should fall in the range 10 to 1000 Hz. */
    return OSTimeGet(&err) * (1000 / OS_CFG_TICK_RATE_HZ);
}

void get_elapsed_frequency(uint64_t * real_freq, uint64_t * kernel_freq, uint64_t * user_freq)
{
    *real_freq = BSP_CPU_ClkFreq();
    *kernel_freq = 0;
    *user_freq = 0;
}

void get_elapsed_counters(uint64_t * real_counter, uint64_t * kernel_counter, uint64_t * user_counter)
{
    *real_counter = CPU_TS_Get64();
}

void update_elapsed_counters()
{
    /* Update the 64-bit counter from the CPU register; call this often to prevent counter overflow. */
    CPU_TS_Update();
}

#elif !defined (NOT_HAVE_SYS_TIME_H)
#include <sys/time.h>
#if !defined(NOT_HAVE_SYS_TIMES_H)
#include <sys/times.h>
#include <unistd.h>
#endif

size_t mkms_time()
{
    struct timeval tm;
    gettimeofday(&tm, NULL);
    return (size_t)(tm.tv_sec * 1000 + tm.tv_usec / 1000);
}

void get_elapsed_counters(uint64_t * real_counter, uint64_t * kernel_counter, uint64_t * user_counter)
{
#if !defined(NOT_HAVE_SYS_TIMES_H)
    struct tms tms;
#endif

    struct timeval tm;
    gettimeofday(&tm, NULL);
    *real_counter = (uint64_t)tm.tv_sec * 1000000 + tm.tv_usec;

#if defined(NOT_HAVE_SYS_TIMES_H)
    *kernel_counter = 0;
    *user_counter = 0;
#else
    if (times(&tms) != (clock_t)-1) {
        *kernel_counter = tms.tms_stime;
        *user_counter = tms.tms_utime;
    }
    else {
        *kernel_counter = 0;
        *user_counter = 0;
    }
#endif
}

void get_elapsed_frequency(uint64_t * real_freq, uint64_t * kernel_freq, uint64_t * user_freq)
{
    *real_freq = 1000000;

#if defined(NOT_HAVE_SYS_TIMES_H)
    *kernel_freq = 0;
    *user_freq = 0;
#else
    *kernel_freq = *user_freq = sysconf(_SC_CLK_TCK);
#endif
}

#else
#include <time.h>

size_t mkms_time()
{
    return time(NULL) * 1000;
}

void get_elapsed_counters(uint64_t * real_counter, uint64_t * kernel_counter, uint64_t * user_counter)
{
    *real_counter = time(NULL);
    *kernel_counter = 0;
    *user_counter = 0;
}

void get_elapsed_frequency(uint64_t * real_freq, uint64_t * kernel_freq, uint64_t * user_freq)
{
    *real_freq = 1;
    *kernel_freq = 0;
    *user_freq = 0;
}

#endif

#if defined(_WIN32_WCE)
int mkms_process_time(size_t* kernel_time, size_t* user_time)
{
    /* TODO: implement mkms_process_time on WinCE */
    *kernel_time = 0;
    *user_time = 0;
    return 0;
}
#elif defined(_WIN32)
int mkms_process_time(size_t* kernel_time, size_t* user_time)
{
    FILETIME creationFileTime, exitFileTime, kernelFileTime, userFileTime;

    if (GetProcessTimes(GetCurrentProcess(), &creationFileTime, &exitFileTime, &kernelFileTime, &userFileTime)) {
        ULARGE_INTEGER kernelIntTime, userIntTime;
        kernelIntTime.HighPart = kernelFileTime.dwHighDateTime;
        kernelIntTime.LowPart = kernelFileTime.dwLowDateTime;
        userIntTime.HighPart = userFileTime.dwHighDateTime;
        userIntTime.LowPart = userFileTime.dwLowDateTime;

        *kernel_time = kernelIntTime.QuadPart / 10000;
        *user_time = userIntTime.QuadPart / 10000;
        return 1;
    }
    else {
        *kernel_time = 0;
        *user_time = 0;
        return 0;
    }
}
#else
int mkms_process_time(size_t* kernel_time, size_t* user_time)
{
    /* TODO: implement mkms_process_time on POSIX */
    *kernel_time = 0;
    *user_time = 0;
    return 0;
}
#endif

#ifndef _WIN32_WCE
void get_current_time(time_t * timestamp, char * timestring, int buffer_length)
{
	time(timestamp);
	strncpy(timestring, ctime(timestamp), buffer_length);
}
#else
void get_current_time(time_t * timestamp, char * timestring, int buffer_length)
{
    int offset = 0;
	wchar_t* wtimebuf = (wchar_t*)malloc(sizeof(wchar_t) * buffer_length);

	/* Get unix time on Windows. See MSDN documentation for RtlTimeToSecondsSince1970. */
	SYSTEMTIME systemtime;
	FILETIME filetime;
	ULARGE_INTEGER current_time, epoch_time;

	/* Calculate time of the UNIX timestamp epoch. */
	systemtime.wYear = 1970;
	systemtime.wMonth = 1;
	systemtime.wDay = 1;
	systemtime.wHour = 0;
	systemtime.wMinute = 0;
	systemtime.wSecond = 1;
	systemtime.wMilliseconds = 0;
	SystemTimeToFileTime(&systemtime, &filetime);
	epoch_time.HighPart = filetime.dwHighDateTime;
	epoch_time.LowPart = filetime.dwLowDateTime;

	/* Get current system time. */
	GetSystemTime(&systemtime);
	SystemTimeToFileTime(&systemtime, &filetime);
	current_time.HighPart = filetime.dwHighDateTime;
	current_time.LowPart = filetime.dwLowDateTime;

	/* Find difference between current time and epoch, and convert to seconds. */
	*timestamp = (time_t) ((current_time.QuadPart - epoch_time.QuadPart) / 10000000);

	GetDateFormat(LOCALE_SYSTEM_DEFAULT, 0, &systemtime, NULL, wtimebuf, buffer_length - 1);
	offset = wcslen(wtimebuf);
	if (offset < buffer_length - 2) {
		wtimebuf[offset++] = ' ';
		wtimebuf[offset] = '\0';
		GetTimeFormat(LOCALE_SYSTEM_DEFAULT, 0, &systemtime, NULL, wtimebuf + offset, buffer_length - 1 - offset);
	}
	wcstombs(timestring, wtimebuf, buffer_length - 1);
    free(wtimebuf);
}
#endif
