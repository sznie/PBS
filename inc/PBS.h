#pragma once
#include "PBSNode.h"
#include "SingleAgentSolver.h"

auto cmp = [](PBSNode* left, PBSNode* right) { 
	// return (left->cost - left->depth > right->cost - right->depth);
	// return left->depth < right->depth;
	// return make_tuple(-left->depth, left->cost, left->conflicts.size()) > make_tuple(-right->depth, right->cost, right->conflicts.size());
	// return make_tuple(left->cost, left->conflicts.size(), -left->depth) > make_tuple(right->cost, right->conflicts.size(), -right->depth);
	return make_tuple(left->conflicts.size(), left->cost, -left->depth) > make_tuple(right->conflicts.size(), right->cost, -right->depth);
	// return make_tuple(50 * left->conflicts.size() + left->cost, -left->depth) > make_tuple(50 * right->conflicts.size() + right->cost, -right->depth); 
	// return left->cost - 50 * std::log(left->depth) > right->cost - 50 * std::log(right->depth) ; 
	};
using NodeQueue = std::priority_queue<PBSNode*, std::vector<PBSNode*>, decltype(cmp)>;

class PBS
{
public:
	/////////////////////////////////////////////////////////////////////////////////////
	// stats
	double runtime = 0;
	double runtime_generate_child = 0; // runtime of generating child nodes
	double runtime_build_CT = 0; // runtime of building constraint table
	double runtime_build_CAT = 0; // runtime of building conflict avoidance table
	double runtime_path_finding = 0; // runtime of finding paths for single agents
	double runtime_detect_conflicts = 0;
	double runtime_preprocessing = 0; // runtime of building heuristic table for the low level
	double runtime_in_cycles = 0; // runtime being spent in cycles
	uint64_t num_cycles_detected = 0;
	uint64_t num_nodes_failed = 0;

	uint64_t num_HL_expanded = 0;
	uint64_t num_HL_generated = 0;
	uint64_t num_LL_expanded = 0;
	uint64_t num_LL_generated = 0;

	PBSNode* dummy_start = nullptr;
    PBSNode* goal_node = nullptr;



	bool solution_found = false;
	int solution_cost = -2;
	int priority_window;
	bool avoidance_variant;

	/////////////////////////////////////////////////////////////////////////////////////////
	// set params
	void setConflictSelectionRule(conflict_selection c) { conflict_seletion_rule = c;}
	void setNodeLimit(int n) { node_limit = n; }

	////////////////////////////////////////////////////////////////////////////////////////////
	// Runs the algorithm until the problem is solved or time is exhausted 
	bool solve(double time_limit);

	PBS(const Instance& instance, bool sipp, int screen, int window, bool avoidance);
	void clearSearchEngines();
	~PBS();

	// Save results
	void saveResults(const string &fileName, const string &instanceName) const;
	void saveCT(const string &fileName) const; // write the CT to a file
    void savePaths(const string &fileName) const; // write the paths to a file
	void clear(); // used for rapid random  restart
private:
	conflict_selection conflict_seletion_rule;

    // stack<PBSNode*> open_list;
	NodeQueue open_list{cmp};
	list<PBSNode*> allNodes_table;


    list<int> ordered_agents;
    vector<vector<vector<bool>>> priority_graph; // [t][i][j] = true indicates that i is lower than j at timestep t

    string getSolverName() const;

	int screen;
	
	double time_limit;
	int node_limit = MAX_NODES;

	clock_t start;

	int num_of_agents;


	vector<Path*> paths;
	vector < SingleAgentSolver* > search_engines;  // used to find (single) agents' paths and mdd

    bool generateChild(int child_id, PBSNode* parent, int low, int high, int conflict_time);
	bool resolveBucket(PBSNode* parent, int low, int high, int bucket, list<tuple<int,int,int>>& buckets_to_replan);
	bool resolveBucketAvoidance(PBSNode* parent, int low, int high, int bucket); 

	int bucketFromTimestep(int timestep) const;
	void updatePriorityGraph(int low, int high, int constraint_time);
	bool hasConflicts(int a1, int a2) const;
    bool hasConflicts(int a1, const set<int>& agents) const;
	bool hasConflictsInBucket(int a1, int a2, int bucket) const;
	void fillConflicts(int a1, int a2, PBSNode &node);
	shared_ptr<Conflict> chooseConflict(PBSNode &node);
    int getSumOfCosts() const;
	inline void releaseNodes();

	// print and save
	void printResults() const;
	static void printConflicts(const PBSNode &curr);
    void printPriorityGraph() const;

	bool validateSolution() const;
	inline int getAgentLocation(int agent_id, size_t timestep) const;

	vector<int> shuffleAgents() const;  //generate random permuattion of agent indices
	bool terminate(PBSNode* curr); // check the stop condition and return true if it meets

    int getHigherPriorityAgents(const list<int>::reverse_iterator & p1, set<int>& agents, int timestep);
    int getLowerPriorityAgents(const list<int>::iterator & p1, set<int>& agents, int timestep);
    bool hasHigherPriority(int low, int high, int timestep) const; // return true if agent low is lower than agent high
	void getHigherPriorityConstraintBuckets(int a, list<pair<int, int>>& conflict_times); // returns list higher agent->time
	void getHigherPriorityConstraintBucketsUtil(int a2, int bucket, list<pair<int, int>>& conflict_buckets);
	void getAvoidanceConstraintBuckets(int a, list<pair<int, int>>& conflict_times);
	bool constraintIsViolated(int a, int agent, int bucket);

	// node operators
	void pushNode(PBSNode* node);
    void pushNodes(PBSNode* n1, PBSNode* n2);
	PBSNode* selectNode();

		 // high level search
	bool generateRoot();
    bool findPathForSingleAgent(PBSNode& node, const set<int>& higher_agents, int a, Path& new_path);
	void classifyConflicts(PBSNode &parent);
	void update(PBSNode* node);
	void printPaths() const;

    int topologicalSort(list<int>& stack, int timestep);
    void topologicalSortUtil(int v, vector<bool> & visited, list<int> & stack, int bucket);
};
