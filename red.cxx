#include <iostream>
#include <string>
#include <vector>
#include <limits>
#include <json.hpp>

using json = nlohmann::json;

enum class Level {
    LOW,
    MEDIUM,
    HIGH
};

enum class Health {
    GOOD,
    DEGRADED,
    BAD
};

struct QueueEvidence {
    std::string id;

    int queue_size;

    double lambda; // tasa de llegada
    double mu;     // tasa de salida
    double rho;    // ocupación

    Level q_level;
    Level lambda_level;
    Level mu_level;
    Level rho_level;

    Level priority;
    Health health;
};

//Discretizacion
Level discretize_queue_size(int q) {
    if (q <= 2) return Level::LOW;
    if (q <= 6) return Level::MEDIUM;
    return Level::HIGH;
}

Level discretize_rate(double value) {
    if (value < 1.0) return Level::LOW;
    if (value <= 3.0) return Level::MEDIUM;
    return Level::HIGH;
}

Level discretize_rho(double rho) {
    if (rho < 0.8) return Level::LOW;
    if (rho <= 1.2) return Level::MEDIUM;
    return Level::HIGH;
}

Level parse_priority(const std::string& p) {
    if (p == "HIGH") return Level::HIGH;
    if (p == "MEDIUM") return Level::MEDIUM;
    return Level::LOW;
}

Health parse_health(const std::string& h) {
    if (h == "GOOD") return Health::GOOD;
    if (h == "DEGRADED") return Health::DEGRADED;
    return Health::BAD;
}

struct QueueStatus {
    std::size_t queue_size = 0;

    std::uint64_t arrivals = 0;
    std::uint64_t departures = 0;

    Health health = Health::GOOD;
};

//Reglas
double probability_assignment_high(const QueueEvidence& e) {
    double p = 0.50;

    // Tamaño de cola
    if (e.q_level == Level::LOW) {
        p += 0.25;
    } else if (e.q_level == Level::MEDIUM) {
        p += 0.05;
    } else {
        p -= 0.30;
    }

    // Ocupación reciente
    if (e.rho_level == Level::LOW) {
        p += 0.20;
    } else if (e.rho_level == Level::MEDIUM) {
        p += 0.05;
    } else {
        p -= 0.30;
    }

    // Salud del servidor
    if (e.health == Health::GOOD) {
        p += 0.20;
    } else if (e.health == Health::DEGRADED) {
        p -= 0.15;
    } else {
        p -= 0.60;
    }

    // Si el trabajo es de alta prioridad, castigamos colas riesgosas
    if (e.priority == Level::HIGH) {
        if (e.q_level != Level::LOW) {
            p -= 0.10;
        }

        if (e.rho_level == Level::HIGH) {
            p -= 0.20;
        }

        if (e.health != Health::GOOD) {
            p -= 0.20;
        }
    }

    if (p < 0.0) p = 0.0;
    if (p > 1.0) p = 1.0;

    return p;
}


//Parseo del json
QueueEvidence build_evidence(
    const json& queue_json,
    int window_ms,
    Level job_priority
) {
    QueueEvidence e;

    e.id = queue_json["id"].get<std::string>();
    e.queue_size = queue_json["queue_size"].get<int>();

    int arrivals = queue_json["arrivals"].get<int>();
    int departures = queue_json["departures"].get<int>();

    double window_seconds = window_ms / 1000.0;

    e.lambda = arrivals / window_seconds;
    e.mu = departures / window_seconds;

    if (e.mu == 0.0) {
        if (e.lambda > 0.0) {
            e.rho = std::numeric_limits<double>::infinity();
        } else {
            e.rho = 0.0;
        }
    } else {
        e.rho = e.lambda / e.mu;
    }

    e.q_level = discretize_queue_size(e.queue_size);
    e.lambda_level = discretize_rate(e.lambda);
    e.mu_level = discretize_rate(e.mu);
    e.rho_level = discretize_rho(e.rho);

    e.priority = job_priority;
    e.health = parse_health(queue_json["health"].get<std::string>());

    return e;
}

std::string choose_queue(const json& input) {
    int window_ms = input["window_ms"].get<int>();

    std::string priority_str = input["job"]["priority"].get<std::string>();
    Level job_priority = parse_priority(priority_str);

    std::string best_queue;
    double best_probability = -1.0;

    for (const auto& q : input["queues"]) {
        QueueEvidence evidence = build_evidence(q, window_ms, job_priority);

        double p_high = probability_assignment_high(evidence);

        std::cout << "Queue: " << evidence.id
                  << " P(A=Alta)=" << p_high
                  << " Qi=" << evidence.queue_size
                  << " lambda=" << evidence.lambda
                  << " mu=" << evidence.mu
                  << " rho=" << evidence.rho
                  << std::endl;

        if (p_high > best_probability) {
            best_probability = p_high;
            best_queue = evidence.id;
        }
    }

    return best_queue;
}

int main() {
    std::string raw_json = R"(
    {
      "window_ms": 5000,
      "job": {
        "id": "TXN-0001",
        "priority": "HIGH"
      },
      "queues": [
        {
          "id": "queue_1",
          "queue_size": 2,
          "arrivals": 10,
          "departures": 14,
          "health": "GOOD"
        },
        {
          "id": "queue_2",
          "queue_size": 5,
          "arrivals": 18,
          "departures": 9,
          "health": "GOOD"
        },
        {
          "id": "queue_3",
          "queue_size": 1,
          "arrivals": 3,
          "departures": 2,
          "health": "DEGRADED"
        }
      ]
    }
    )";

    json input = json::parse(raw_json);

    std::string selected_queue = choose_queue(input);

    std::cout << "Selected queue: " << selected_queue << std::endl;

    return 0;
}