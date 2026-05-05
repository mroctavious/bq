#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <fstream>
#include <limits>
#include <mutex>
#include <optional>
#include <random>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

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

    // Conteos observados. Para la primera integración pueden ser acumulados
    // si después deseas una ventana móvil, sólo debes alimentar estos campos
    // con los conteos de la ventana reciente.
    std::uint64_t arrivals = 0;
    std::uint64_t departures = 0;

    Health health = Health::GOOD;
};

struct DiscreteRange {
    double low_max = 0.0;
    double medium_max = 0.0;
};

struct SelectionConfig {
    std::size_t default_sample_size = 3;

    std::size_t low_priority_sample_size = 2;
    std::size_t medium_priority_sample_size = 3;
    std::size_t high_priority_sample_size = 5;
};

struct BayesianConfig {
    double window_seconds = 5.0;

    DiscreteRange queue_size_range {2.0, 6.0};
    DiscreteRange rate_range {1.0, 3.0};
    DiscreteRange rho_range {0.8, 1.2};

    SelectionConfig selection;

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
        boost::property_tree::ptree root;

        try {
            boost::property_tree::read_json(filename, root);
        } catch (const std::exception&) {
            return false;
        }

        BayesianConfig new_config;

        const auto bayesian_child = root.get_child_optional("bayesian");
        const boost::property_tree::ptree* cfg =
            bayesian_child ? &bayesian_child.get() : &root;

        new_config.window_seconds =
            cfg->get<int>("window_ms", 5000) / 1000.0;

        if (new_config.window_seconds <= 0.0) {
            new_config.window_seconds = 1.0;
        }

        new_config.queue_size_range.low_max =
            cfg->get<double>("discretization.queue_size.low_max", 2.0);
        new_config.queue_size_range.medium_max =
            cfg->get<double>("discretization.queue_size.medium_max", 6.0);

        new_config.rate_range.low_max =
            cfg->get<double>("discretization.rate.low_max", 1.0);
        new_config.rate_range.medium_max =
            cfg->get<double>("discretization.rate.medium_max", 3.0);

        new_config.rho_range.low_max =
            cfg->get<double>("discretization.rho.low_max", 0.8);
        new_config.rho_range.medium_max =
            cfg->get<double>("discretization.rho.medium_max", 1.2);

        new_config.selection.default_sample_size =
            cfg->get<std::size_t>("selection.default_sample_size", 3);
        new_config.selection.low_priority_sample_size =
            cfg->get<std::size_t>("selection.sample_size_by_priority.LOW", 2);
        new_config.selection.medium_priority_sample_size =
            cfg->get<std::size_t>("selection.sample_size_by_priority.MEDIUM", 3);
        new_config.selection.high_priority_sample_size =
            cfg->get<std::size_t>("selection.sample_size_by_priority.HIGH", 5);

        if (new_config.selection.default_sample_size == 0) {
            new_config.selection.default_sample_size = 1;
        }
        if (new_config.selection.low_priority_sample_size == 0) {
            new_config.selection.low_priority_sample_size = 1;
        }
        if (new_config.selection.medium_priority_sample_size == 0) {
            new_config.selection.medium_priority_sample_size = 1;
        }
        if (new_config.selection.high_priority_sample_size == 0) {
            new_config.selection.high_priority_sample_size = 1;
        }

        new_config.base_score = cfg->get<double>("score.base", 0.50);

        new_config.queue_size_score = parse_level_scores(*cfg, "score.queue_size");
        new_config.rho_score = parse_level_scores(*cfg, "score.rho");
        new_config.health_score = parse_health_scores(*cfg, "score.health");

        new_config.penalty_if_queue_not_low =
            cfg->get<double>("score.priority_high.penalty_if_queue_not_low", -0.10);
        new_config.penalty_if_rho_high =
            cfg->get<double>("score.priority_high.penalty_if_rho_high", -0.20);
        new_config.penalty_if_health_not_good =
            cfg->get<double>("score.priority_high.penalty_if_health_not_good", -0.20);

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
            const auto index = it->second;
            queues_[index].status = status;

            if (!queues_[index].active) {
                queues_[index].active = true;
                active_position_[index] = active_indices_.size();
                active_indices_.push_back(index);
            }

            return index;
        }

        QueueEntry entry;
        entry.name = queue_name;
        entry.index = queues_.size();
        entry.status = status;
        entry.active = true;

        queues_.push_back(entry);
        name_to_index_[queue_name] = entry.index;

        active_position_[entry.index] = active_indices_.size();
        active_indices_.push_back(entry.index);

        return entry.index;
    }

    bool remove_queue(const std::string& queue_name) {
        std::unique_lock lock(mutex_);

        auto it = name_to_index_.find(queue_name);

        if (it == name_to_index_.end()) {
            return false;
        }

        const std::size_t index = it->second;

        queues_[index].active = false;
        name_to_index_.erase(it);

        auto pos_it = active_position_.find(index);

        if (pos_it != active_position_.end()) {
            const std::size_t pos = pos_it->second;
            const std::size_t last_index = active_indices_.back();

            active_indices_[pos] = last_index;
            active_position_[last_index] = pos;

            active_indices_.pop_back();
            active_position_.erase(index);
        }

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
        return choose_queue(priority, [](std::size_t, const QueueStatus& stored) {
            return stored;
        });
    }

    // Versión recomendada para integrarse con el simulador JSQ:
    // el selector muestrea parcialmente y sólo pide el estado actual
    // de las colas candidatas seleccionadas.
    template <typename StatusProvider>
    std::optional<std::size_t> choose_queue(
        Priority priority,
        StatusProvider&& status_provider
    ) const {
        std::shared_lock lock(mutex_);

        if (!loaded_ || active_indices_.empty()) {
            return std::nullopt;
        }

        std::size_t requested_sample_size =
            sample_size_for_priority(priority, config_);

        if (requested_sample_size == 0) {
            requested_sample_size = 1;
        }

        const std::size_t sample_size =
            std::min(requested_sample_size, active_indices_.size());

        double best_probability = -1.0;
        std::optional<std::size_t> best_index;
        std::size_t best_queue_size = std::numeric_limits<std::size_t>::max();

        if (sample_size == active_indices_.size()) {
            for (const auto queue_index : active_indices_) {
                evaluate_candidate(
                    queue_index,
                    priority,
                    status_provider,
                    best_probability,
                    best_index,
                    best_queue_size
                );
            }

            return best_index;
        }

        thread_local std::mt19937 rng {std::random_device{}()};
        std::uniform_int_distribution<std::size_t> dist(
            0,
            active_indices_.size() - 1
        );

        std::unordered_set<std::size_t> sampled_positions;
        sampled_positions.reserve(sample_size);

        while (sampled_positions.size() < sample_size) {
            const std::size_t pos = dist(rng);

            if (!sampled_positions.insert(pos).second) {
                continue;
            }

            const std::size_t queue_index = active_indices_[pos];

            evaluate_candidate(
                queue_index,
                priority,
                status_provider,
                best_probability,
                best_index,
                best_queue_size
            );
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
    static std::array<double, 3> parse_level_scores(
        const boost::property_tree::ptree& root,
        const std::string& path
    ) {
        return {
            root.get<double>(path + ".LOW", 0.0),
            root.get<double>(path + ".MEDIUM", 0.0),
            root.get<double>(path + ".HIGH", 0.0)
        };
    }

    static std::array<double, 3> parse_health_scores(
        const boost::property_tree::ptree& root,
        const std::string& path
    ) {
        return {
            root.get<double>(path + ".GOOD", 0.0),
            root.get<double>(path + ".DEGRADED", 0.0),
            root.get<double>(path + ".BAD", 0.0)
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

    static std::size_t sample_size_for_priority(
        Priority priority,
        const BayesianConfig& config
    ) {
        switch (priority) {
            case Priority::LOW:
                return config.selection.low_priority_sample_size;
            case Priority::MEDIUM:
                return config.selection.medium_priority_sample_size;
            case Priority::HIGH:
                return config.selection.high_priority_sample_size;
        }

        return config.selection.default_sample_size;
    }

    static QueueEvidence build_evidence(
        const QueueEntry& queue,
        const QueueStatus& status,
        Priority priority,
        const BayesianConfig& config
    ) {
        QueueEvidence evidence;

        evidence.name = queue.name;
        evidence.index = queue.index;
        evidence.status = status;
        evidence.priority = priority;

        evidence.lambda =
            static_cast<double>(status.arrivals) / config.window_seconds;

        evidence.mu =
            static_cast<double>(status.departures) / config.window_seconds;

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
            discretize(static_cast<double>(status.queue_size), config.queue_size_range);

        evidence.lambda_level = discretize(evidence.lambda, config.rate_range);
        evidence.mu_level = discretize(evidence.mu, config.rate_range);
        evidence.rho_level = discretize(evidence.rho, config.rho_range);

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

    template <typename StatusProvider>
    void evaluate_candidate(
        std::size_t queue_index,
        Priority priority,
        StatusProvider& status_provider,
        double& best_probability,
        std::optional<std::size_t>& best_index,
        std::size_t& best_queue_size
    ) const {
        const auto& queue = queues_[queue_index];

        QueueStatus current_status = status_provider(queue.index, queue.status);
        QueueEvidence evidence = build_evidence(queue, current_status, priority, config_);
        double probability = probability_assignment_high(evidence, config_);

        bool better = false;

        if (probability > best_probability) {
            better = true;
        } else if (probability == best_probability &&
                   evidence.status.queue_size < best_queue_size) {
            better = true;
        } else if (probability == best_probability &&
                   evidence.status.queue_size == best_queue_size &&
                   (!best_index.has_value() || queue.index < *best_index)) {
            better = true;
        }

        if (better) {
            best_probability = probability;
            best_index = queue.index;
            best_queue_size = evidence.status.queue_size;
        }
    }

private:
    mutable std::shared_mutex mutex_;

    BayesianConfig config_;
    bool loaded_ = false;

    std::vector<QueueEntry> queues_;
    std::unordered_map<std::string, std::size_t> name_to_index_;

    std::vector<std::size_t> active_indices_;
    std::unordered_map<std::size_t, std::size_t> active_position_;
};
