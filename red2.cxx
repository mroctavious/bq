#include <iostream>
#include <BayesianQueueSelector.hpp>




int main() {
    BayesianQueueSelector selector;

    if (!selector.load("bayesian_config.json")) {
        std::cerr << "Error loading bayesian_config.json\n";
        return 1;
    }

    QueueStatus q0;
    q0.queue_size = 2;
    q0.arrivals = 10;
    q0.departures = 14;
    q0.health = Health::BAD;

    QueueStatus q1;
    q1.queue_size = 5;
    q1.arrivals = 18;
    q1.departures = 9;
    q1.health = Health::GOOD;

    QueueStatus q2;
    q2.queue_size = 1;
    q2.arrivals = 3;
    q2.departures = 2;
    q2.health = Health::DEGRADED;

    selector.add_queue("queue_0", q0);
    selector.add_queue("queue_1", q1);
    selector.add_queue("queue_2", q2);

    auto selected_index = selector.choose_queue(Priority::HIGH);

    if (selected_index.has_value()) {
        std::cout << "Selected queue index: "
                  << *selected_index
                  << std::endl;
    } else {
        std::cout << "No queue selected\n";
    }

    return 0;
}