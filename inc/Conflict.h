#pragma once
#include "common.h"

enum conflict_selection {RANDOM, EARLIEST, CONFLICTS, MCONSTRAINTS, FCONSTRAINTS, WIDTH, SINGLETONS};

struct Constraint
{
    int low = -1;
    int high = -1;
    int conflict_time = -1;
    void set(int _low, int _high, int _conflict_time)
    {
        low = _low;
        high = _high;
        conflict_time = _conflict_time;
    }
};

std::ostream& operator<<(std::ostream& os, const Constraint& constraint);


struct Conflict
{
	int a1;
	int a2;
    int timestep;
    explicit Conflict(int a1 = -1, int a2 = -1, int timestep = -1): a1(a1), a2(a2), timestep(timestep) { }
};

std::ostream& operator<<(std::ostream& os, const Conflict& conflict);

bool operator < (const Conflict& conflict1, const Conflict& conflict2);
