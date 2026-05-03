#include <algorithm>
#include <array>
#include <cstdint>
#include <fstream>
#include <limits>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <json.hpp>


using json = nlohmann::json;

enum class Level {
    LOW = 0,
    MEDIUM = 1,
    HIGH = 2
};

enum class Health {
    GOOD = 0,
    DEGRADED = 1,
    BAD = 2
};

enum class Priority {
    LOW = 0,
    MEDIUM = 1,
    HIGH = 2
};

struct QueueStatus {
    std::size_t queue_size = 0;

    std::uint64_t arrivals = 0;
    std::uint64_t departures = 0;

    Health health = Health::GOOD;
};

struct DiscreteRange {
    double low_max = 0.0;
    double medium_max = 0.0;
};

struct BayesianConfig {
    double window_seconds = 5.0;

    DiscreteRange queue_size_range;
    DiscreteRange rate_range;
    DiscreteRange rho_range;

    double base_score = 0.50;

    std::array<double, 3> queue_size_score {0.25, 0.05, -0.30};
    std::array<double, 3> rho_score        {0.20, 0.05, -0.30};
    std::array<double, 3> health_score     {0.20, -0.15, -0.60};

    double penalty_if_queue_not_low = -0.10;
    double penalty_if_rho_high = -0.20;
    double penalty_if_health_not_good = -0.20;
};

struct QueueEvidence {
    std::string name;
    std::size_t index = 0;

    QueueStatus status;

    double lambda = 0.0;
    double mu = 0.0;
    double rho = 0.0;

    Level q_level = Level::LOW;
    Level lambda_level = Level::LOW;
    Level mu_level = Level::LOW;
    Level rho_level = Level::LOW;

    Priority priority = Priority::MEDIUM;
};

class BayesianQueueSelector {
public:
    bool load(const std::string& filename) {
        std::ifstream file(filename);

        if (!file.is_open()) {
            return false;
        }

        json j;
        file >> j;

        BayesianConfig new_config;

        new_config.window_seconds =
            j.value("window_ms", 5000) / 1000.0;

        const auto& disc = j.at("discretization");

        new_config.queue_size_range.low_max =
            disc.at("queue_size").value("low_max", 2.0);

        new_config.queue_size_range.medium_max =
            disc.at("queue_size").value("medium_max", 6.0);

        new_config.rate_range.low_max =
            disc.at("rate").value("low_max", 1.0);

        new_config.rate_range.medium_max =
            disc.at("rate").value("medium_max", 3.0);

        new_config.rho_range.low_max =
            disc.at("rho").value("low_max", 0.8);

        new_config.rho_range.medium_max =
            disc.at("rho").value("medium_max", 1.2);

        if (j.contains("score")) {
            const auto& score = j.at("score");

            new_config.base_score = score.value("base", 0.50);

            if (score.contains("queue_size")) {
                new_config.queue_size_score = parse_level_scores(score.at("queue_size"));
            }

            if (score.contains("rho")) {
                new_config.rho_score = parse_level_scores(score.at("rho"));
            }

            if (score.contains("health")) {
                new_config.health_score = parse_health_scores(score.at("health"));
            }

            if (score.contains("priority_high")) {
                const auto& ph = score.at("priority_high");

                new_config.penalty_if_queue_not_low =
                    ph.value("penalty_if_queue_not_low", -0.10);

                new_config.penalty_if_rho_high =
                    ph.value("penalty_if_rho_high", -0.20);

                new_config.penalty_if_health_not_good =
                    ph.value("penalty_if_health_not_good", -0.20);
            }
        }

        {
            std::unique_lock lock(mutex_);
            config_ = new_config;
            loaded_ = true;
        }

        return true;
    }

    std::size_t add_queue(
        const std::string& queue_name,
        const QueueStatus& status
    ) {
        std::unique_lock lock(mutex_);

        auto it = name_to_index_.find(queue_name);

        if (it != name_to_index_.end()) {
            queues_[it->second].status = status;
            queues_[it->second].active = true;
            return it->second;
        }

        QueueEntry entry;
        entry.name = queue_name;
        entry.index = queues_.size();
        entry.status = status;
        entry.active = true;

        queues_.push_back(entry);
        name_to_index_[queue_name] = entry.index;

        return entry.index;
    }

    bool remove_queue(const std::string& queue_name) {
        std::unique_lock lock(mutex_);

        auto it = name_to_index_.find(queue_name);

        if (it == name_to_index_.end()) {
            return false;
        }

        std::size_t index = it->second;

        queues_[index].active = false;
        name_to_index_.erase(it);

        return true;
    }

    bool update(
        const std::string& queue_name,
        const QueueStatus& status
    ) {
        std::unique_lock lock(mutex_);

        auto it = name_to_index_.find(queue_name);

        if (it == name_to_index_.end()) {
            return false;
        }

        queues_[it->second].status = status;

        return true;
    }

    std::optional<std::size_t> choose_queue(
        Priority priority = Priority::MEDIUM
    ) const {
        std::shared_lock lock(mutex_);

        if (!loaded_) {
            return std::nullopt;
        }

        double best_probability = -1.0;
        std::optional<std::size_t> best_index;

        std::size_t best_queue_size = std::numeric_limits<std::size_t>::max();

        for (const auto& queue : queues_) {
            if (!queue.active) {
                continue;
            }

            QueueEvidence evidence =
                build_evidence(queue, priority, config_);

            double probability =
                probability_assignment_high(evidence, config_);

            /*
             * Regla de desempate:
             * 1. Mayor probabilidad.
             * 2. Si empatan, menor tamaño de cola.
             * 3. Si empatan, menor índice.
             */
            bool better = false;

            if (probability > best_probability) {
                better = true;
            } else if (probability == best_probability &&
                       evidence.status.queue_size < best_queue_size) {
                better = true;
            }

            if (better) {
                best_probability = probability;
                best_index = queue.index;
                best_queue_size = evidence.status.queue_size;
            }
        }

        return best_index;
    }

    std::optional<std::string> choose_queue_name(
        Priority priority = Priority::MEDIUM
    ) const {
        auto index = choose_queue(priority);

        if (!index.has_value()) {
            return std::nullopt;
        }

        std::shared_lock lock(mutex_);

        if (*index >= queues_.size() || !queues_[*index].active) {
            return std::nullopt;
        }

        return queues_[*index].name;
    }

private:
    struct QueueEntry {
        std::string name;
        std::size_t index = 0;
        QueueStatus status;
        bool active = true;
    };

private:
    static std::array<double, 3> parse_level_scores(const json& j) {
        return {
            j.value("LOW", 0.0),
            j.value("MEDIUM", 0.0),
            j.value("HIGH", 0.0)
        };
    }

    static std::array<double, 3> parse_health_scores(const json& j) {
        return {
            j.value("GOOD", 0.0),
            j.value("DEGRADED", 0.0),
            j.value("BAD", 0.0)
        };
    }

    static std::size_t level_index(Level level) {
        return static_cast<std::size_t>(level);
    }

    static std::size_t health_index(Health health) {
        return static_cast<std::size_t>(health);
    }

    static Level discretize(double value, const DiscreteRange& range) {
        if (value <= range.low_max) {
            return Level::LOW;
        }

        if (value <= range.medium_max) {
            return Level::MEDIUM;
        }

        return Level::HIGH;
    }

    static QueueEvidence build_evidence(
        const QueueEntry& queue,
        Priority priority,
        const BayesianConfig& config
    ) {
        QueueEvidence evidence;

        evidence.name = queue.name;
        evidence.index = queue.index;
        evidence.status = queue.status;
        evidence.priority = priority;

        evidence.lambda =
            queue.status.arrivals / config.window_seconds;

        evidence.mu =
            queue.status.departures / config.window_seconds;

        if (evidence.mu == 0.0) {
            if (evidence.lambda > 0.0) {
                evidence.rho = std::numeric_limits<double>::infinity();
            } else {
                evidence.rho = 0.0;
            }
        } else {
            evidence.rho = evidence.lambda / evidence.mu;
        }

        evidence.q_level =
            discretize(
                static_cast<double>(queue.status.queue_size),
                config.queue_size_range
            );

        evidence.lambda_level =
            discretize(evidence.lambda, config.rate_range);

        evidence.mu_level =
            discretize(evidence.mu, config.rate_range);

        evidence.rho_level =
            discretize(evidence.rho, config.rho_range);

        return evidence;
    }

    static double probability_assignment_high(
        const QueueEvidence& evidence,
        const BayesianConfig& config
    ) {
        double p = config.base_score;

        p += config.queue_size_score[level_index(evidence.q_level)];
        p += config.rho_score[level_index(evidence.rho_level)];
        p += config.health_score[health_index(evidence.status.health)];

        if (evidence.priority == Priority::HIGH) {
            if (evidence.q_level != Level::LOW) {
                p += config.penalty_if_queue_not_low;
            }

            if (evidence.rho_level == Level::HIGH) {
                p += config.penalty_if_rho_high;
            }

            if (evidence.status.health != Health::GOOD) {
                p += config.penalty_if_health_not_good;
            }
        }

        return std::clamp(p, 0.0, 1.0);
    }

private:
    mutable std::shared_mutex mutex_;

    BayesianConfig config_;
    bool loaded_ = false;

    std::vector<QueueEntry> queues_;
    std::unordered_map<std::string, std::size_t> name_to_index_;
};
