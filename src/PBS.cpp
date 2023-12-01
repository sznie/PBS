#include <algorithm>    // std::shuffle
#include <random>      // std::default_random_engine
#include <chrono>       // std::chrono::system_clock
#include "PBS.h"
#include "SIPP.h"
#include "SpaceTimeAStar.h"

int priority_max_time = 1000;

PBS::PBS(const Instance& instance, bool sipp, int screen, int window) :
        screen(screen),
        priority_window(window),
        num_of_agents(instance.getDefaultNumberOfAgents())
{
    clock_t t = clock();

    search_engines.resize(num_of_agents);
    for (int i = 0; i < num_of_agents; i++)
    {
        if (sipp)
            search_engines[i] = new SIPP(instance, i);
        else
            search_engines[i] = new SpaceTimeAStar(instance, i);
    }
    runtime_preprocessing = (double)(clock() - t) / CLOCKS_PER_SEC;

    if (screen >= 2) // print start and goals
    {
        instance.printAgents();
    }
}


bool PBS::solve(double _time_limit)
{
    this->time_limit = _time_limit;

    if (screen > 0) // 1 or 2
    {
        string name = getSolverName();
        name.resize(35, ' ');
        cout << name << ": ";
    }
    // set timer
    start = clock();

    generateRoot();

    while (!open_list.empty())
    {
        auto curr = selectNode();

        if (terminate(curr)) break;

        curr->conflict = chooseConflict(*curr);

        if (screen > 1)
            cout << "	Expand " << *curr << "	on " << *(curr->conflict) << " b=" << bucketFromTimestep(curr->conflict->timestep) << endl;

        assert(hasConflictsInBucket(curr->conflict->a1, curr->conflict->a2, bucketFromTimestep(curr->conflict->timestep)));
        assert(!hasHigherPriority(curr->conflict->a1, curr->conflict->a2, curr->conflict->timestep) and
               !hasHigherPriority(curr->conflict->a2, curr->conflict->a1, curr->conflict->timestep) );
        auto t1 = clock();
        vector<Path*> copy(paths);
        generateChild(0, curr, curr->conflict->a1, curr->conflict->a2, curr->conflict->timestep);
        paths = copy;
        generateChild(1, curr, curr->conflict->a2, curr->conflict->a1, curr->conflict->timestep);
        runtime_generate_child += (double)(clock() - t1) / CLOCKS_PER_SEC;
        pushNodes(curr->children[0], curr->children[1]);
        curr->clear();
    }  // end of while loop
    return solution_found;
}

bool PBS::resolveBucket(PBSNode* node, int low, int high, int bucket, list<tuple<int,int,int>>& buckets_to_replan) {
    int conflict_time = bucket * priority_window;
    // all topological sorting is for this bucket only
    topologicalSort(ordered_agents, conflict_time);
    if (screen > 2)
    {
        cout << "Ordered agents in bucket " << bucket << ": ";
        for (int i : ordered_agents)
            cout << i << ",";
        cout << endl;
    }
    vector<int> topological_orders(num_of_agents); // map agent i to its position in ordered_agents
    auto i = num_of_agents - 1;
    for (const auto & a : ordered_agents)
    {
        topological_orders[a] = i;
        i--;
    }

    std::priority_queue<pair<int, int>> to_replan; // <position in ordered_agents, agent id>
    vector<bool> lookup_table(num_of_agents, false); // whether this agent is in the to_replan queue
    to_replan.emplace(topological_orders[low], low);
    lookup_table[low] = true;
    { // find conflicts where one agent is higher than high and the other agent is lower than low in this bucket
        set<int> higher_agents;
        auto p = ordered_agents.rbegin();
        std::advance(p, topological_orders[high]);
        assert(*p == high);
        getHigherPriorityAgents(p, higher_agents, conflict_time);
        higher_agents.insert(high);

        set<int> lower_agents;
        auto p2 = ordered_agents.begin();
        std::advance(p2, num_of_agents - 1 - topological_orders[low]);
        assert(*p2 == low);
        getLowerPriorityAgents(p2, lower_agents, conflict_time);

        for (const auto & conflict : node->conflicts)
        {
            int a1 = conflict->a1;
            int a2 = conflict->a2;
            int b = bucketFromTimestep(conflict->timestep);
            if (a1 == low or a2 == low)
                continue;
            if (topological_orders[a1] > topological_orders[a2])
            {
                std::swap(a1, a2);
            }
            if (b == bucket and !lookup_table[a1] and lower_agents.find(a1) != lower_agents.end() and higher_agents.find(a2) != higher_agents.end())
            {
                assert(hasConflictsInBucket(a1, a2, bucket));
                // cout << "to replan " << a1 << " " << a2 << " b=" << bucket << endl;
                to_replan.emplace(topological_orders[a1], a1);
                lookup_table[a1] = true;
            }
        }
    }


    // resolve this bucket
    while(!to_replan.empty())
    {
        int a, rank;
        tie(rank, a) = to_replan.top();
        to_replan.pop();
        lookup_table[a] = false;
        if (screen > 2) cout << "Replan agent " << a << endl;
        // Re-plan path
        set<int> higher_agents;
        auto p = ordered_agents.rbegin();
        std::advance(p, rank);
        assert(*p == a);
        bucket = getHigherPriorityAgents(p, higher_agents, conflict_time);
        assert(!higher_agents.empty());
        if (screen > 2)
        {
            cout << "Higher agents in bucket " << bucket << ": ";
            for (auto i : higher_agents)
                cout << i << ",";
            cout << endl;
        }
        Path new_path;
        if(!findPathForSingleAgent(*node, higher_agents, a, new_path))
        {
            return false;
        }
        for (int a2 : higher_agents) {
            assert(!hasConflictsInBucket(a, a2, bucket));
        }

        // Delete old conflicts
        for (auto c = node->conflicts.begin(); c != node->conflicts.end();)
        {
            if (((*c)->a1 == a or (*c)->a2 == a) and (bucketFromTimestep((*c)->timestep) == bucketFromTimestep(conflict_time))) {
                c = node->conflicts.erase(c);
            } else if (!hasConflictsInBucket((*c)->a1, (*c)->a2, bucketFromTimestep((*c)->timestep))) {
                c = node->conflicts.erase(c);
            } else {
                ++c;
            }
        }

        // Update conflicts and to_replan
        set<int> lower_agents;
        auto p2 = ordered_agents.begin();
        std::advance(p2, num_of_agents - 1 - rank);
        assert(*p2 == a);
        bucket = getLowerPriorityAgents(p2, lower_agents, conflict_time);
        if (screen > 2)
        {
            cout << "Lower agents in bucket " << bucket << ": ";
            for (auto i : lower_agents)
                cout << i << ",";
            cout << endl;
        }

        // Find new conflicts
        for (auto a2 = 0; a2 < num_of_agents; a2++)
        {
            // if a2>a in this bucket, it is not necessarily true for all other buckts
            if (a2 == a or lookup_table[a2]) // already in to_replan
                continue;
            auto t = clock();
            // assert if higher_agents.count(a2) > 0, then the conflicts should NOT be in this bucket
            if (higher_agents.count(a2) > 0) {
                assert(!hasConflictsInBucket(a, a2, bucket));
            }
            for (int b = 0; b < (int) priority_max_time / priority_window; b++) {
                if (hasConflictsInBucket(a, a2, b))
                {
                    // cout << "found conflict " << a << " " << a2<< " b=" << b << endl;
                    node->conflicts.emplace_back(new Conflict(a, a2, b*priority_window));
                    // if working in this bucket
                    if (b == bucket) {
                        if (lower_agents.count(a2) > 0) // has a collision with a lower priority agent (bucket's constraints are violated)
                        {
                            if (screen > 1)
                                cout << "\t" << a2 << " needs to be replanned due to collisions with " << a << endl;
                            to_replan.emplace(topological_orders[a2], a2);
                            lookup_table[a2] = true;
                        }
                    } else {
                        // Replan bucket if topological sort is violated, not just between a and a2
                        if (constraintIsViolated(a, a2, b)) {
                            buckets_to_replan.emplace_back(make_tuple(b, a, a2));
                        }
                        if (constraintIsViolated(a2, a, b)) {
                            buckets_to_replan.emplace_back(make_tuple(b, a2, a));
                        }
                    }
                }
            }
            runtime_detect_conflicts += (double)(clock() - t) / CLOCKS_PER_SEC;
        }
    }
    // cout << "done resolving b=" << bucket << " " << low << "<" << high << endl;
    return true;
}

bool PBS::generateChild(int child_id, PBSNode* parent, int low, int high, int conflict_time)
{
    assert(child_id == 0 or child_id == 1);
    parent->children[child_id] = new PBSNode(*parent);
    auto node = parent->children[child_id];
    node->constraint.set(low, high, conflict_time);

    // contribution: update priority graph for the window number of timesteps in bucket where conflict falls
    updatePriorityGraph(low, high, conflict_time);

    if (screen > 2)
        printPriorityGraph();

    std::list<tuple<int, int, int>> buckets_to_replan; // <bucket, low, high>
    buckets_to_replan.push_back(make_tuple(bucketFromTimestep(conflict_time), low, high));
    vector<int> counts;
    counts.assign(num_of_agents, 0);
    // while a bucket's constraints is violated:
    //    resolve bucket
    //    check constraints in other buckets are violated
    auto t1 = clock();
    while(!buckets_to_replan.empty())
    {
        int bucket, agent1, agent2;
        tie(bucket, agent1, agent2) = buckets_to_replan.front();
        buckets_to_replan.pop_front();
        if (screen > 2) {
            cout << "   Resolve bucket " << agent1 << "<" << agent2 << " in b=" << bucket << "/ replan: ";
            for (auto i : buckets_to_replan)
                cout << "(" << get<0>(i) << "," << get<1>(i) <<"," << get<2>(i) << "),";
            cout << endl;
        }
        if (!hasConflictsInBucket(agent1, agent2, bucket)) continue;
        if (counts[agent1] > 5) {

            if (screen > 2) {
                cout << "Node " << parent->time_generated << "   CYCLE DETECTED: Agent " << agent1 << endl;
            }
            runtime_in_cycles += (double)(clock() - t1) / CLOCKS_PER_SEC;
            num_cycles_detected += 1;
            delete node;
            parent->children[child_id] = nullptr;
            return false;
        }
        counts[agent1] += 1;
        
        int resolve_bucket = resolveBucket(node, agent1, agent2, bucket, buckets_to_replan);
        if (!resolve_bucket) {
            delete node;
            parent->children[child_id] = nullptr;
            return false;
        }
    }
    num_HL_generated++;
    node->time_generated = num_HL_generated;
    if (screen > 1)
        cout << "Generated " << *node << endl << endl;
    return true;
}

bool PBS::findPathForSingleAgent(PBSNode& node, const set<int>& higher_agents, int a, Path& new_path)
{
    clock_t t = clock();
    // agent is constrained to avoid agents for buckets of time
    list <pair<int, int>> conflict_buckets;
    getHigherPriorityConstraintBuckets(a, conflict_buckets);
    new_path = search_engines[a]->findOptimalPath(higher_agents, paths, a, priority_window, conflict_buckets);  //TODO: add runtime check to the low level
    num_LL_expanded += search_engines[a]->num_expanded;
    num_LL_generated += search_engines[a]->num_generated;
    runtime_build_CT += search_engines[a]->runtime_build_CT;
    runtime_build_CAT += search_engines[a]->runtime_build_CAT;
    runtime_path_finding += (double)(clock() - t) / CLOCKS_PER_SEC;
    if (new_path.empty())
        return false;
    assert(paths[a] != nullptr and !isSamePath(*paths[a], new_path));
    node.cost += (int)new_path.size() - (int)paths[a]->size();
    if (node.makespan >= paths[a]->size())
    {
        node.makespan = max(node.makespan, new_path.size() - 1);
    }
    else
    {
        node.makespan = 0;
        for (int i = 0; i < num_of_agents; i++)
        {
            if (i == a and new_path.size() - 1 > node.makespan)
                node.makespan = new_path.size() - 1;
            else
                node.makespan = max(node.makespan, paths[i]->size() - 1);
        }
    }
    // path for agent may be replanned more than once per node so must remove outdated path first
    node.paths.remove_if([a](const std::pair<int, Path>& p) { return p.first == a; });
    node.paths.emplace_back(a, new_path);
    paths[a] = &node.paths.back().second;
    for (pair<int, int> a2 : conflict_buckets)
    {
        // the new path found for a avoids higher agent a2.first in bucket a2.second
        // cout << "   has conflict " << a << " " << a2.first << " b=" << a2.second << endl;
        assert(!hasConflictsInBucket(a, a2.first, a2.second));
    }
    return true;
}

inline int PBS::bucketFromTimestep(int timestep) const {
    return (int) (timestep / priority_window);
}

inline void PBS::updatePriorityGraph(int low, int high, int constraint_time) {
    // Calculate bucket of priority graph that timestep falls in
    int bucket = bucketFromTimestep(constraint_time);
    priority_graph[bucket][low][high] = true;
    // assert(!priority_graph[bucket][high][low]);
    priority_graph[bucket][high][low] = false;
}

// takes the paths_found_initially and UPDATE all (constrained) paths found for agents from curr to start
inline void PBS::update(PBSNode* node)
{
    paths.assign(num_of_agents, nullptr);
    int num_priority_buckets = (int) priority_max_time / priority_window;
    priority_graph.assign(num_priority_buckets, vector<vector<bool>>(num_of_agents, vector<bool>(num_of_agents, false)));
    
    for (auto curr = node; curr != nullptr; curr = curr->parent)
	{
        // cout << curr->constraint << " " << *curr << endl;
        for (auto & path : curr->paths)
		{
			if (paths[path.first] == nullptr)
            {
                paths[path.first] = &(path.second); // <int, Path>
			}
		}
        if (curr->parent != nullptr) {
            // non-root node
            updatePriorityGraph(curr->constraint.low, curr->constraint.high, curr->constraint.conflict_time);
        }
	}

    assert(getSumOfCosts() == node->cost);
}

bool PBS::hasConflicts(int a1, int a2) const
{
	int min_path_length = (int) (paths[a1]->size() < paths[a2]->size() ? paths[a1]->size() : paths[a2]->size());
	for (int timestep = 0; timestep < min_path_length; timestep++)
	{
		int loc1 = paths[a1]->at(timestep).location;
		int loc2 = paths[a2]->at(timestep).location;
		if (loc1 == loc2 or (timestep < min_path_length - 1 and loc1 == paths[a2]->at(timestep + 1).location
                             and loc2 == paths[a1]->at(timestep + 1).location)) // vertex or edge conflict
		{
            return true;
		}
	}
	if (paths[a1]->size() != paths[a2]->size())
	{
		int a1_ = paths[a1]->size() < paths[a2]->size() ? a1 : a2;
		int a2_ = paths[a1]->size() < paths[a2]->size() ? a2 : a1;
		int loc1 = paths[a1_]->back().location;
		for (int timestep = min_path_length; timestep < (int)paths[a2_]->size(); timestep++)
		{
			int loc2 = paths[a2_]->at(timestep).location;
			if (loc1 == loc2)
			{
				return true; // target conflict
			}
		}
	}
    return false; // conflict-free
}

bool PBS::hasConflictsInBucket(int a1, int a2, int bucket) const
{
    int min_path_length = (int) (paths[a1]->size() < paths[a2]->size() ? paths[a1]->size() : paths[a2]->size());
    int start_timestep = priority_window * bucket;
    int last_timestep = start_timestep + priority_window;
    
	for (int timestep = start_timestep; (timestep < last_timestep) and (timestep < min_path_length); timestep++)
	{
		int loc1 = paths[a1]->at(timestep).location;
		int loc2 = paths[a2]->at(timestep).location;
		if (loc1 == loc2 or (timestep < min_path_length - 1 and loc1 == paths[a2]->at(timestep + 1).location
                             and loc2 == paths[a1]->at(timestep + 1).location)) // vertex or edge conflict
		{
            // cout << "Conflict reg bucket " << bucket << " " << a1 << ", " << a2 << " " << loc1 << " t=" << timestep << endl;
            return true;
		}
	}
	// some parts of the bucket fall outside of min_path_length
    if ((paths[a1]->size() != paths[a2]->size()) and last_timestep > min_path_length)
	{
		int a1_ = paths[a1]->size() < paths[a2]->size() ? a1 : a2; //shorter path
		int a2_ = paths[a1]->size() < paths[a2]->size() ? a2 : a1; //longer path
		int loc1 = paths[a1_]->back().location;
		for (int timestep = max(start_timestep, min_path_length); timestep < (int)paths[a2_]->size() and timestep < last_timestep; timestep++)
		{
			int loc2 = paths[a2_]->at(timestep).location;
			if (loc1 == loc2)
			{
				// cout << "Conflict bucket " << bucket << " a1=" << a1 << " length " << paths[a1]->size() << ", a2=" << a2 << " length " << paths[a2]->size() << ", at " << loc1 << " t=" << timestep << " [" << start_timestep << "," << last_timestep << "]" << endl;
                return true; // target conflict
			}
		}
	}
    // bucket is outside of both paths, make sure goal locations are different
    if (paths[a1]->size() <= start_timestep and paths[a2]->size() <= start_timestep) {
        int loc1 = paths[a1]->back().location;
        int loc2 = paths[a2]->back().location;
        if (loc1 == loc2)
        {
            // cout << "Conflict outside " << bucket << " a=" << a1 << " length " << paths[a1]->size() << ", a2=" << a2 << " length " << paths[a2]->size() << " " << loc1 << endl;
            return true; // target conflict
        }
    }
    return false; // conflict-free
}

// same as has conflicts but it fills the conflict array
void PBS::fillConflicts(int a1, int a2, PBSNode &node)
{
	int min_path_length = (int) (paths[a1]->size() < paths[a2]->size() ? paths[a1]->size() : paths[a2]->size());
	for (int timestep = 0; timestep < min_path_length; timestep++)
	{
		int loc1 = paths[a1]->at(timestep).location;
		int loc2 = paths[a2]->at(timestep).location;
		if (loc1 == loc2 or (timestep < min_path_length - 1 and loc1 == paths[a2]->at(timestep + 1).location
                             and loc2 == paths[a1]->at(timestep + 1).location)) // vertex or edge conflict
		{
            node.conflicts.emplace_back(new Conflict(a1, a2, timestep));
		}
	}
	if (paths[a1]->size() != paths[a2]->size())
	{
		int a1_ = paths[a1]->size() < paths[a2]->size() ? a1 : a2;
		int a2_ = paths[a1]->size() < paths[a2]->size() ? a2 : a1;
		int loc1 = paths[a1_]->back().location;
		for (int timestep = min_path_length; timestep < (int)paths[a2_]->size(); timestep++)
		{
			int loc2 = paths[a2_]->at(timestep).location; // target conflict
			if (loc1 == loc2)
			{
                node.conflicts.emplace_back(new Conflict(a1, a2, timestep));
			}
		}
	}
    return; // conflict-free
}

bool PBS::hasConflicts(int a1, const set<int>& agents) const
{
    for (auto a2 : agents)
    {
        if (hasConflicts(a1, a2))
            return true;
    }
    return false;
}
shared_ptr<Conflict> PBS::chooseConflict(const PBSNode &node) const
{
	if (screen == 3)
		printConflicts(node);
	if (node.conflicts.empty())
		return nullptr;
    return node.conflicts.back();
}
int PBS::getSumOfCosts() const
{
   int cost = 0;
   for (const auto & path : paths)
       cost += (int)path->size() - 1;
   return cost;
}
inline void PBS::pushNode(PBSNode* node)
{
	// update handles
    open_list.push(node);
	allNodes_table.push_back(node);
}
void PBS::pushNodes(PBSNode* n1, PBSNode* n2)
{
    if (n1 != nullptr and n2 != nullptr)
    {
        if (n1->cost < n2->cost)
        {
            pushNode(n2);
            pushNode(n1);
        }
        else
        {
            pushNode(n1);
            pushNode(n2);
        }
    }
    else if (n1 != nullptr)
    {
        pushNode(n1);
    }
    else if (n2 != nullptr)
    {
        pushNode(n2);
    }
}

PBSNode* PBS::selectNode()
{
	PBSNode* curr = open_list.top();
    open_list.pop();
    update(curr);
    num_HL_expanded++;
    curr->time_expanded = num_HL_expanded;
	if (screen > 1)
		cout << endl << "Pop " << *curr << endl;
	return curr;
}

void PBS::printPaths() const
{
    for (int i = 0; i < num_of_agents; i++)
	{
        if (i != 35 and i != 30) {
            continue;
        }
		cout << "Agent " << i << " (" << search_engines[i]->my_heuristic[search_engines[i]->start_location] << " -->" <<
			paths[i]->size() - 1 << "): ";
		for (const auto & t : *paths[i])
			cout << t.location << "->";
		cout << endl;
	}
}

void PBS::printPriorityGraph() const
{
    cout << "Priority graph:";
    for (int bucket = 0; bucket < (int) priority_max_time / priority_window; bucket++) {
        for (int a1 = 0; a1 < num_of_agents; a1++)
        {
            for (int a2 = 0; a2 < num_of_agents; a2++)
            {
                if (priority_graph[bucket][a1][a2]) {
                    cout << a1 << "<" << a2 << " in bucket=" << bucket << ", ";
                }    
            }
        }
    }
    cout << endl;
}

void PBS::printResults() const
{
	if (solution_cost >= 0) // solved
		cout << "Succeed,";
	else if (solution_cost == -1) // time_out
		cout << "Timeout,";
	else if (solution_cost == -2) // no solution
		cout << "No solutions,";
	else if (solution_cost == -3) // nodes out
		cout << "Nodesout,";

	cout << solution_cost << "," << runtime << "," <<
         num_HL_expanded << "," << num_LL_expanded << "," << // HL_num_generated << "," << LL_num_generated << "," <<
		dummy_start->cost << "," << endl;
    /*if (solution_cost >= 0) // solved
    {
        cout << "fhat = [";
        auto curr = goal_node;
        while (curr != nullptr)
        {
            cout << curr->getFHatVal() << ",";
            curr = curr->parent;
        }
        cout << "]" << endl;
        cout << "hhat = [";
        curr = goal_node;
        while (curr != nullptr)
        {
            cout << curr->cost_to_go << ",";
            curr = curr->parent;
        }
        cout << "]" << endl;
        cout << "d = [";
        curr = goal_node;
        while (curr != nullptr)
        {
            cout << curr->distance_to_go << ",";
            curr = curr->parent;
        }
        cout << "]" << endl;
        cout << "soc = [";
        curr = goal_node;
        while (curr != nullptr)
        {
            cout << curr->getFHatVal() - curr->cost_to_go << ",";
            curr = curr->parent;
        }
        cout << "]" << endl;
    }*/
}

void PBS::saveResults(const string &fileName, const string &instanceName) const
{
	std::ifstream infile(fileName);
	bool exist = infile.good();
	infile.close();
	if (!exist)
	{
		ofstream addHeads(fileName);
		addHeads << "runtime,#high-level expanded,#high-level generated,#low-level expanded,#low-level generated," <<
			"num agents,priority window,solution cost,root g value," <<
            "runtime in cycles,num cycles detected," <<
			"runtime of detecting conflicts,runtime of building constraint tables,runtime of building CATs," <<
			"runtime of path finding,runtime of generating child nodes," <<
			"preprocessing runtime,solver name,instance name" << endl;
		addHeads.close();
	}
	ofstream stats(fileName, std::ios::app);
	stats << runtime << "," <<
          num_HL_expanded << "," << num_HL_generated << "," <<
          num_LL_expanded << "," << num_LL_generated << "," <<

          num_of_agents << "," << priority_window << "," << solution_cost << "," << dummy_start->cost << "," <<
          runtime_in_cycles << "," << num_cycles_detected << "," <<

		runtime_detect_conflicts << "," << runtime_build_CT << "," << runtime_build_CAT << "," <<
		runtime_path_finding << "," << runtime_generate_child << "," <<

		runtime_preprocessing << "," << getSolverName() << "," << instanceName << endl;
	stats.close();
}

void PBS::saveCT(const string &fileName) const // write the CT to a file
{
	// Write the tree graph in dot language to a file
	{
		std::ofstream output;
		output.open(fileName + ".tree", std::ios::out);
		output << "digraph G {" << endl;
		output << "size = \"5,5\";" << endl;
		output << "center = true;" << endl;
		set<PBSNode*> path_to_goal;
		auto curr = goal_node;
		while (curr != nullptr)
		{
			path_to_goal.insert(curr);
			curr = curr->parent;
		}
		for (const auto& node : allNodes_table)
		{
			output << node->time_generated << " [label=\"g=" << node->cost;
			if (node->time_expanded > 0) // the node has been expanded
			{
				output << "\n #" << node->time_expanded;
			}
			output << "\"]" << endl;


			if (node == dummy_start)
				continue;
			if (path_to_goal.find(node) == path_to_goal.end())
			{
				output << node->parent->time_generated << " -> " << node->time_generated << endl;
			}
			else
			{
				output << node->parent->time_generated << " -> " << node->time_generated << " [color=red]" << endl;
			}
		}
		output << "}" << endl;
		output.close();
	}

	// Write the stats of the tree to a CSV file
	{
		std::ofstream output;
		output.open(fileName + "-tree.csv", std::ios::out);
		// header
		output << "time generated,g value,h value,h^ value,d value,depth,time expanded,chosen from,h computed,"
			<< "f of best in cleanup,f^ of best in cleanup,d of best in cleanup,"
			<< "f of best in open,f^ of best in open,d of best in open,"
			<< "f of best in focal,f^ of best in focal,d of best in focal,"
			<< "praent,goal node" << endl;
		for (auto& node : allNodes_table)
		{
			output << node->time_generated << ","
                   << node->cost << ","
				<< node->depth << ","
				<< node->time_expanded << ",";
			if (node->parent == nullptr)
				output << "0,";
			else
				output << node->parent->time_generated << ",";
			if (node == goal_node)
				output << "1" << endl;
			else
				output << "0" << endl;
		}
		output.close();
	}

}

void PBS::savePaths(const string &fileName) const
{
    std::ofstream output;
    output.open(fileName, std::ios::out);
    for (int i = 0; i < num_of_agents; i++)
    {
        output << "Agent " << i << ": ";
        for (const auto & t : *paths[i])
            output << "(" << search_engines[0]->instance.getRowCoordinate(t.location)
                   << "," << search_engines[0]->instance.getColCoordinate(t.location) << ")->";
        output << endl;
    }
    output.close();
}

void PBS::printConflicts(const PBSNode &curr)
{
	for (const auto& conflict : curr.conflicts)
	{
		cout << "conflict: " << *conflict << " b=" << (int) (conflict->timestep) / 1 << endl;;
	}
}


string PBS::getSolverName() const
{
	return "PBS with " + search_engines[0]->getName();
}


bool PBS::terminate(PBSNode* curr)
{
	runtime = (double)(clock() - start) / CLOCKS_PER_SEC;
	if (curr->conflicts.empty()) //no conflicts
	{// found a solution
		solution_found = true;
		goal_node = curr;
		solution_cost = goal_node->cost;
		if (!validateSolution())
		{
			cout << "Solution invalid!!!" << endl;
			printPaths();
			exit(-1);
		}
		if (screen > 0) // 1 or 2
			printResults();
		return true;
	}
	if (runtime > time_limit || num_HL_expanded > node_limit)
	{   // time/node out
		solution_cost = -1;
		solution_found = false;
        if (screen > 0) // 1 or 2
            printResults();
		return true;
	}
	return false;
}


bool PBS::generateRoot()
{
	auto root = new PBSNode();
	root->cost = 0;
	paths.reserve(num_of_agents);

    set<int> higher_agents;
    for (auto i = 0; i < num_of_agents; i++)
    {
        //CAT cat(dummy_start->makespan + 1);  // initialized to false
        //updateReservationTable(cat, i, *dummy_start);
        list <pair<int, int>> conflict_list;
        auto new_path = search_engines[i]->findOptimalPath(higher_agents, paths, i, priority_window, conflict_list);
        num_LL_expanded += search_engines[i]->num_expanded;
        num_LL_generated += search_engines[i]->num_generated;
        if (new_path.empty())
        {
            cout << "No path exists for agent " << i << endl;
            return false;
        }
        root->paths.emplace_back(i, new_path);
        paths.emplace_back(&root->paths.back().second);
        root->makespan = max(root->makespan, new_path.size() - 1);
        root->cost += (int)new_path.size() - 1;
    }
    auto t = clock();
	root->depth = 0;
    for (int a1 = 0; a1 < num_of_agents; a1++)
    {
        for (int a2 = a1 + 1; a2 < num_of_agents; a2++)
        {
            fillConflicts(a1, a2, *root);
        }
    }
    runtime_detect_conflicts += (double)(clock() - t) / CLOCKS_PER_SEC;
    num_HL_generated++;
    root->time_generated = num_HL_generated;
    if (screen > 1)
        cout << "Generate Root " << *root << endl;
	pushNode(root);
	dummy_start = root;
	if (screen >= 2) // print start and goals
	{
		printPaths();
	}

	return true;
}

inline void PBS::releaseNodes()
{
    // TODO:: clear open_list
	for (auto& node : allNodes_table)
		delete node;
	allNodes_table.clear();
}



/*inline void PBS::releaseOpenListNodes()
{
	while (!open_list.empty())
	{
		PBSNode* curr = open_list.top();
		open_list.pop();
		delete curr;
	}
}*/

PBS::~PBS()
{
	releaseNodes();
}

void PBS::clearSearchEngines()
{
	for (auto s : search_engines)
		delete s;
	search_engines.clear();
}


bool PBS::validateSolution() const
{
	// check whether the paths are feasible
	size_t soc = 0;
	for (int a1 = 0; a1 < num_of_agents; a1++)
	{
		soc += paths[a1]->size() - 1;
		for (int a2 = a1 + 1; a2 < num_of_agents; a2++)
		{
			size_t min_path_length = paths[a1]->size() < paths[a2]->size() ? paths[a1]->size() : paths[a2]->size();
			for (size_t timestep = 0; timestep < min_path_length; timestep++)
			{
				int loc1 = paths[a1]->at(timestep).location;
				int loc2 = paths[a2]->at(timestep).location;
				if (loc1 == loc2)
				{
					cout << "Agents " << a1 << " and " << a2 << " collides at " << loc1 << " at timestep " << timestep << endl;
					return false;
				}
				else if (timestep < min_path_length - 1
					&& loc1 == paths[a2]->at(timestep + 1).location
					&& loc2 == paths[a1]->at(timestep + 1).location)
				{
					cout << "Agents " << a1 << " and " << a2 << " collides at (" <<
						loc1 << "-->" << loc2 << ") at timestep " << timestep << endl;
					return false;
				}
			}
			if (paths[a1]->size() != paths[a2]->size())
			{
				int a1_ = paths[a1]->size() < paths[a2]->size() ? a1 : a2;
				int a2_ = paths[a1]->size() < paths[a2]->size() ? a2 : a1;
				int loc1 = paths[a1_]->back().location;
				for (size_t timestep = min_path_length; timestep < paths[a2_]->size(); timestep++)
				{
					int loc2 = paths[a2_]->at(timestep).location;
					if (loc1 == loc2)
					{
						cout << "Agents " << a1 << " and " << a2 << " collides at " << loc1 << " at timestep " << timestep << endl;
						return false; // It's at least a semi conflict			
					}
				}
			}
		}
	}
	if ((int)soc != solution_cost)
	{
		cout << "The solution cost is wrong!" << endl;
		return false;
	}
	return true;
}

inline int PBS::getAgentLocation(int agent_id, size_t timestep) const
{
	size_t t = max(min(timestep, paths[agent_id]->size() - 1), (size_t)0);
	return paths[agent_id]->at(t).location;
}


// used for rapid random  restart
void PBS::clear()
{
	releaseNodes();
	paths.clear();
	dummy_start = nullptr;
	goal_node = nullptr;
	solution_found = false;
	solution_cost = -2;
}


int PBS::topologicalSort(list<int>& stack, int timestep)
{
    stack.clear();
    vector<bool> visited(num_of_agents, false);
    int bucket = bucketFromTimestep(timestep);

    // Call the recursive helper function to store Topological
    // Sort starting from all vertices one by one
    for (int i = 0; i < num_of_agents; i++)
    {
        if (!visited[i])
            topologicalSortUtil(i, visited, stack, bucket);
    }
    return bucket;
}
void PBS::topologicalSortUtil(int v, vector<bool> & visited, list<int> & stack, int bucket)
{
    // Mark the current node as visited.
    visited[v] = true;

    // Recur for all the vertices adjacent to this vertex
    assert(!priority_graph.empty());
    for (int i = 0; i < num_of_agents; i++)
    {
        if (priority_graph[bucket][v][i] and !visited[i])
            topologicalSortUtil(i, visited, stack, bucket);
    }
    // Push current vertex to stack which stores result
    stack.push_back(v);
}
int PBS::getHigherPriorityAgents(const list<int>::reverse_iterator & p1, set<int>& higher_agents, int timestep)
{
    int bucket = bucketFromTimestep(timestep);
    for (auto p2 = std::next(p1); p2 != ordered_agents.rend(); ++p2)
    {
        if (priority_graph[bucket][*p1][*p2])
        {
            auto ret = higher_agents.insert(*p2);
            if (ret.second) // insert successfully
            {
                getHigherPriorityAgents(p2, higher_agents, timestep);
            }
        }
    }
    return bucket;
}
int PBS::getLowerPriorityAgents(const list<int>::iterator & p1, set<int>& lower_subplans, int timestep)
{
    int bucket = bucketFromTimestep(timestep);
    for (auto p2 = std::next(p1); p2 != ordered_agents.end(); ++p2)
    {
        if (priority_graph[bucket][*p2][*p1])
        {
            auto ret = lower_subplans.insert(*p2);
            if (ret.second) // insert successfully
            {
                getLowerPriorityAgents(p2, lower_subplans, timestep);
            }
        }
    }
    return bucket;
}

bool PBS::hasHigherPriority(int low, int high, int timestep) const // return true if agent low is lower than agent high
{
    std::queue<int> Q;
    vector<bool> visited(num_of_agents, false);
    visited[low] = false;
    Q.push(low);
    int bucket = bucketFromTimestep(timestep);
    while(!Q.empty())
    {
        auto n = Q.front();
        Q.pop();
        if (n == high)
            return true;
        for (int i = 0; i < num_of_agents; i++)
        {
            if (priority_graph[bucket][n][i] and !visited[i])
                Q.push(i);
        }
    }
    return false;
}

// return buckets where agent a must be constrained
void PBS::getHigherPriorityConstraintBuckets(int a, list<pair<int, int>>& conflict_buckets) {
    int num_buckets = (int) priority_max_time / priority_window;
    // cout << "Higher priority constraint buckets" << endl;
    for (int bucket = 0; bucket < num_buckets; bucket++) {
        for (int a2 = 0; a2 < num_of_agents; a2++)
        {
            if (priority_graph[bucket][a][a2]) {
                // cout << "   constrained: " << a << "<" << a2 << " in bucket " << bucket << endl;
                conflict_buckets.push_back(make_pair(a2, bucket));
                getHigherPriorityConstraintBucketsUtil(a2, bucket, conflict_buckets);
            } 
        }
    }
}

// return buckets where agent a must be constrained
void PBS::getHigherPriorityConstraintBucketsUtil(int a2, int bucket, list<pair<int, int>>& conflict_buckets) {
    for (int a3 = 0; a3 < num_of_agents; a3++)
    {
        if (priority_graph[bucket][a2][a3]) {
            conflict_buckets.push_back(make_pair(a3, bucket));
            getHigherPriorityConstraintBucketsUtil(a3, bucket, conflict_buckets);
        }
    }
}

// return buckets where agent a must be constrained
bool PBS::constraintIsViolated(int a, int agent, int bucket) {
    for (int a2 = 0; a2 < num_of_agents; a2++)
    {
        if (priority_graph[bucket][a][a2]) {
            if (a2 == agent) return true;
            if (constraintIsViolated(a2, agent, bucket)) return true;
        } 
    }
    return false;
}