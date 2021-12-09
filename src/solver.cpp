#include <cstdint>

#include "ortools/graph/min_cost_flow.h"

namespace operations_research {
void MaxFlowSolver() {
  SimpleMinCostFlow min_cost_flow;

  // Get constants (regions, cost, capacity)
  const std::vector<std::string> regions = GetRegions();
  const std::vector<std::vector<double>> cost_matrix = GetCostMatrix();
  const std::vector<std::vector<double>> capacity_matrix = GetCapacityMatrix();

  std::vector<int64_t> start_nodes;
  std::vector<int64_t> end_nodes;
  std::vector<int64_t> capacities;
  std::vector<double> unit_costs;

  for (int i = 0; i < regions.size(); ++i) {
    for (int j = 0; j < regions.size(); ++j) {
      if (i != j) {
        start_nodes.push_back(i);
        end_nodes.push_back(j);
        capacities.push_back(capacity_matrix[i][j]);
        unit_costs.push_back(cost_matrix[i][j]);
      }
    }
    supplies.push_back(0);
  }

  // Add the arcs.
  for (int i = 0; i < start_nodes.size(); ++i) {
    int arc = min_cost_flow.AddArcWithCapacityAndUnitCost(
        start_nodes[i], end_nodes[i], capacities[i], unit_costs[i]);
    if (arc != i)
      LOG(FATAL) << "Internal error";
  }

  int status = min_cost_flow.Solve();

  if (status == MinCostFlow::OPTIMAL) {
    LOG(INFO) << "Minimum cost flow: " << min_cost_flow.OptimalCost();
    LOG(INFO) << "";
    LOG(INFO) << " Arc   Flow / Capacity  Cost";
    for (std::size_t i = 0; i < min_cost_flow.NumArcs(); ++i) {
      int64_t cost = min_cost_flow.Flow(i) * min_cost_flow.UnitCost(i);
      LOG(INFO) << min_cost_flow.Tail(i) << " -> " << min_cost_flow.Head(i)
                << "  " << min_cost_flow.Flow(i) << "  / "
                << min_cost_flow.Capacity(i) << "       " << cost;
    }
  } else {
    LOG(INFO) << "Solving the min cost flow problem failed. Solver status: "
              << status;
  }
}

} // namespace operations_research

int main() {
  operations_research::SimpleMinCostFlowProgram();
  return EXIT_SUCCESS;
}