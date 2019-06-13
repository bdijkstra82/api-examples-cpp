#ifndef __TIME_COUNTER_H__
#define __TIME_COUNTER_H__

#if !defined(_MSC_VER) || (_MSC_VER >= 1600 && !defined(_WIN32_WCE))
#ifndef __STDC_CONSTANT_MACROS
#define __STDC_CONSTANT_MACROS
#endif
#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif
#include <stdint.h>
#else
#include <limits.h>
#ifndef UINT64_MAX
#define UINT64_MAX 0xFFFFFFFFFFFFFFFF
typedef unsigned __int64     uint64_t;
#endif
#endif

#include <stdlib.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Returns current time in millisecond units. Used for measuring time spans.
 */
size_t mkms_time();

/**
 * Obtains elapsed process times in millisecond units.
 */
int mkms_process_time(size_t* kernel_time, size_t* user_time);

/**
 * Count elapsed time in platform-specific counter ticks.
 */
void get_elapsed_counters(uint64_t* real_counter, uint64_t* kernel_counter, uint64_t* user_counter);

/**
 * Get counter ticks per second for the current platform.
 */
void get_elapsed_frequency(uint64_t* real_freq, uint64_t* kernel_freq, uint64_t* user_freq);

/**
 * Obtains current time as a UNIX timestamp and platform-dependent time string.
 */
void get_current_time(time_t * timestamp, char * timestring, int buffer_length);

#ifdef __cplusplus
}

#include <assert.h>

class TimeCounter {
public:
	TimeCounter()
		: start_time(0)
		, start_kernel_time(0)
		, start_user_time(0)
		, elapsed_time(0)
		, elapsed_kernel_time(0)
		, elapsed_user_time(0)
		, last_interval(0)
		, min_interval(UINT64_MAX)
		, max_interval(0)
	{
		get_elapsed_frequency(&freq, &kernel_freq, &user_freq);
	}

	TimeCounter(const TimeCounter& other)
		: start_time(0)
		, start_kernel_time(0)
		, start_user_time(0)
		, elapsed_time(other.elapsed_time)
		, elapsed_kernel_time(other.elapsed_kernel_time)
		, elapsed_user_time(other.elapsed_user_time)
		, last_interval(other.last_interval)
		, min_interval(other.min_interval)
		, max_interval(other.max_interval)
		, freq(other.freq)
		, kernel_freq(other.kernel_freq)
		, user_freq(other.user_freq)
	{
	}

	/** Start the counter. */
	void start() { get_elapsed_counters(&start_time, &start_kernel_time, &start_user_time); }
	/** Stop the counter and add accumulated time to elapsed time. */
	void stop()
	{
		uint64_t real_time, kernel_time, user_time;
		get_elapsed_counters(&real_time, &kernel_time, &user_time);
		if (start_time) { record_interval(real_time - start_time); start_time = 0; }
		if (start_kernel_time) { elapsed_kernel_time += kernel_time - start_kernel_time; start_kernel_time = 0; }
		if (start_user_time) { elapsed_user_time += user_time - start_user_time; start_user_time = 0; }
	}
	/** Clear elapsed time and stop the counter. */
	void reset() { elapsed_time = start_time = start_kernel_time = start_user_time = elapsed_kernel_time = elapsed_user_time = 0; min_interval = UINT64_MAX; max_interval = 0; }
	/** Update elapsed time without stopping the counter. */
	void update()
	{
		uint64_t real_time, kernel_time, user_time;
		get_elapsed_counters(&real_time, &kernel_time, &user_time);
		if (start_time) { record_interval(real_time - start_time); start_time = real_time; }
		if (start_kernel_time) { elapsed_kernel_time += kernel_time - start_kernel_time; start_kernel_time = kernel_time; }
		if (start_user_time) { elapsed_user_time += user_time - start_user_time; start_user_time = user_time; }
	}

	/** Obtain elapsed time in milliseconds. @see stop, update */
	size_t elapsed_milliseconds() const { return (size_t)(elapsed_time * 1000 / freq); }
	/** Obtain elapsed time in seconds. @see stop, update */
	double elapsed_seconds() const { return ((double) elapsed_time) / freq; }

	/** Obtain elapsed kernel execution time in milliseconds. @see stop, update */
	size_t elapsed_kernel_milliseconds() const { return (size_t)(elapsed_kernel_time * 1000 / kernel_freq); }
	/** Obtain elapsed kernel execution time in seconds. @see stop, update */
	double elapsed_kernel_seconds() const { return ((double) elapsed_kernel_time) / kernel_freq; }

	/** Obtain elapsed user execution time in milliseconds. @see stop, update */
	size_t elapsed_user_milliseconds() const { return (size_t)(elapsed_user_time * 1000 / user_freq); }
	/** Obtain elapsed user execution time in seconds. @see stop, update */
	double elapsed_user_seconds() const { return ((double) elapsed_user_time) / kernel_freq; }

	/** Obtain last measured time in milliseconds. @see stop, update */
	size_t last_milliseconds() const { return (size_t)(last_interval * 1000 / freq); }
	/** Obtain last measured time in seconds. @see stop, update */
	double last_seconds() const { return ((double) last_interval) / freq; }

	/** Obtain maximum individual measurement in milliseconds. */
	size_t max_milliseconds() const { return (size_t)(max_interval * 1000 / freq); }
	/** Obtain maximum individual measurement in seconds. */
	double max_seconds() const { return ((double) max_interval) / freq; }

	/** Obtain minimum individual measurement in milliseconds. */
	size_t min_milliseconds() const { return min_interval == UINT64_MAX ? 0 : (size_t)(min_interval * 1000 / freq); }
	/** Obtain minimum individual measurement in seconds. */
	double min_seconds() const { return min_interval == UINT64_MAX ? 0.0 :((double) min_interval) / freq; }

	bool is_last_max() const { return last_interval == max_interval; }
	bool is_last_min() const { return last_interval == min_interval; }

	/** Return true if kernel and user time are available. */
	bool have_process_execution_time() const { return kernel_freq != 0 && user_freq != 0; }

	TimeCounter& operator+=(const TimeCounter& other)
	{
		assert(freq == other.freq);
		assert(kernel_freq == other.kernel_freq);
		assert(user_freq == other.user_freq);

		/* Accumulate elapsed times. */
		elapsed_time += other.elapsed_time;
		elapsed_kernel_time += other.elapsed_kernel_time;
		elapsed_user_time += other.elapsed_user_time;

		/* Find common minimum and maximum values. */
		if (min_interval > other.min_interval)
			min_interval = other.min_interval;
		if (max_interval < other.max_interval)
			max_interval = other.max_interval;

		return *this;
	}

	const TimeCounter operator+(const TimeCounter& other)
	{
		return TimeCounter(*this) += other;
	}

private:
	void record_interval(uint64_t interval)
	{
		last_interval = interval;
		elapsed_time += interval;
		if (min_interval > interval)
			min_interval = interval;
		if (max_interval < interval)
			max_interval = interval;
	}

	uint64_t start_time, start_kernel_time, start_user_time;
	uint64_t elapsed_time, elapsed_kernel_time, elapsed_user_time;
	uint64_t last_interval;
	uint64_t min_interval, max_interval;
	uint64_t freq, kernel_freq, user_freq;
};

class AutoTimer {
public:
	AutoTimer(TimeCounter& time_counter)
		: time_counter(time_counter)
		{ time_counter.start(); }

	~AutoTimer()
		{ time_counter.stop(); }

private:
	TimeCounter& time_counter;
};

#endif

#endif
