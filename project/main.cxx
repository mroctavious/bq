#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <random>
#include <algorithm>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <cmath>
#include <cassert>
#include <queue>
#include <functional>
#include <numeric>
#include <optional>
#include <omp.h>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <random>
#include <cstdint>
#include <stdexcept>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <random>
#include <cstdint>
#include <stdexcept>
#include <experimental/filesystem>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "BayesianQueueSelector.hpp"
namespace fs = std::experimental::filesystem;


struct Job{
    int id;
    int worktime;

    static Job create(int id, int wtime){
        return {id, wtime};
    }
};

class Random {
public:
    static void seed(std::uint64_t s) { engine().seed(s); }

    static int randint(int min, int max) {
        if (min > max) throw std::invalid_argument("min > max");
        std::uniform_int_distribution<int> dist(min, max);
        return dist(engine());
    }

private:
    static std::mt19937_64& engine() {
        thread_local std::mt19937_64 eng{std::random_device{}()};
        return eng;
    }
};

struct Qmeta{
    int id;
    std::atomic<int> weight{0};
    std::atomic<int> size{0};
    std::atomic<bool> finished{false};
    std::atomic<std::uint64_t> arrivals{0};
    std::atomic<std::uint64_t> departures{0};
};

template <typename T>
struct SharedQueue {
public:

    void push(const T& value) {
        std::unique_lock<std::mutex> lock(mutex);
        if (closed) {
            return;
        }
        queue.push(value);
        lock.unlock();
        condition.notify_one();
    }

    std::optional<T> pop() {
        std::unique_lock<std::mutex> lock(mutex);
        condition.wait(lock, [this] { return closed || !queue.empty(); });
        if (queue.empty()) {
            return std::nullopt;
        }
        T frontValue = queue.front();
        queue.pop();
        return frontValue;
    }

    std::vector<T> close_and_drain() {
        std::vector<T> drained;
        std::unique_lock<std::mutex> lock(mutex);
        closed = true;

        while (!queue.empty()) {
            drained.push_back(queue.front());
            queue.pop();
        }

        lock.unlock();
        condition.notify_all();
        return drained;
    }

    SharedQueue() = default;

private:
    std::queue<T> queue;
    std::mutex mutex;
    std::condition_variable condition;
    bool closed = false;
};


using JobQueue = std::pair<SharedQueue<Job>, Qmeta>;
using QueueList = std::vector<std::unique_ptr<JobQueue>>;

QueueStatus make_queue_status(const QueueList& qlist, std::size_t qidx) {
    const auto& qinfo = qlist.at(qidx)->second;

    QueueStatus status;
    status.queue_size = static_cast<std::size_t>(
        std::max(0, qinfo.size.load(std::memory_order_relaxed))
    );
    status.arrivals = qinfo.arrivals.load(std::memory_order_relaxed);
    status.departures = qinfo.departures.load(std::memory_order_relaxed);
    status.health = Health::GOOD;

    return status;
}

static constexpr int JSQ = 0;
static constexpr int LSL = 1;
static constexpr int BAYESIAN_PARTIAL = 2;
static constexpr int POWER_OF_2 = 3;
int algorithm;

struct SimulationConfig {
    int algorithm = BAYESIAN_PARTIAL;
    std::uint64_t seed = 87621977;
    int nqueues = 16384;
    int max_queue_size = 100;
    int nworkers = 2;
    int njobs = 10000000;
    int nsimulations = 1000000;
    double simulation_seconds = 60.0;
    int arrival_interval_us = 0;
    int min_w = 1;
    int max_w = 200;
};

std::string algorithm_name(int value) {
    switch (value) {
        case JSQ:
            return "JSQ";
        case LSL:
            return "LSL";
        case BAYESIAN_PARTIAL:
            return "BAYES_PARTIAL";
        case POWER_OF_2:
            return "POWER_OF_2";
        default:
            return "UNKNOWN";
    }
}

int parse_algorithm(const std::string& value) {
    if (value == "0" || value == "JSQ") return JSQ;
    if (value == "1" || value == "LSL" || value == "LWL") return LSL;
    if (value == "2" || value == "BAYESIAN_PARTIAL" || value == "BAYES_PARTIAL") return BAYESIAN_PARTIAL;
    if (value == "3" || value == "POWER_OF_2" || value == "POWEROF2") return POWER_OF_2;
    return std::stoi(value);
}

SimulationConfig load_simulation_config(const std::string& filename) {
    SimulationConfig config;
    boost::property_tree::ptree root;

    boost::property_tree::read_json(filename, root);
    const auto simulation_child = root.get_child_optional("simulation");
    const boost::property_tree::ptree* sim =
        simulation_child ? &simulation_child.get() : &root;

    config.algorithm = parse_algorithm(sim->get<std::string>("algorithm", algorithm_name(config.algorithm)));
    config.seed = sim->get<std::uint64_t>("seed", config.seed);
    config.nqueues = sim->get<int>("nqueues", config.nqueues);
    config.max_queue_size = sim->get<int>("max_queue_size", config.max_queue_size);
    config.nworkers = sim->get<int>("nworkers", config.nworkers);
    config.njobs = sim->get<int>("njobs", config.njobs);
    config.nsimulations = sim->get<int>("nsimulations", config.nsimulations);
    config.simulation_seconds = sim->get<double>("simulation_seconds", config.simulation_seconds);
    config.arrival_interval_us = sim->get<int>("arrival_interval_us", config.arrival_interval_us);
    config.min_w = sim->get<int>("min_w", config.min_w);
    config.max_w = sim->get<int>("max_w", config.max_w);

    return config;
}


int get_lsl_idx(auto& qlist){
    int qidx = 0;
    int jobs_remaining = qlist.at(qidx)->second.weight;

    for(size_t i=1; i<qlist.size(); i++){
        auto& [squeue, qinfo] = *(qlist.at(i));
        if( jobs_remaining > qinfo.weight){
            jobs_remaining = qinfo.weight;
            qidx=i;
        }
    }
    return qidx;
}

static inline long long now_us() {
    using namespace std::chrono;
    return duration_cast<microseconds>(
        steady_clock::now().time_since_epoch()
    ).count();
}

long long START_TIME;

inline void log_event(std::ofstream& out,
                      const char* event,
                      int job_id,
                      int worktime,
                      int qid,
                      int worker_id,
                      int qsize,
                      int qweight,
                      const std::string& extra = "")
{
    out << now_us() - START_TIME << ','
        << event << ','
        << job_id << ','
        << worktime << ','
        << qid << ','
        << worker_id << ','
        << qsize << ','
        << qweight << ','
        << extra
        << '\n';
}

void simple_worker(int worker_id, int qidx, QueueList* qlist, const std::string& log_dir){
    auto& qinfo = qlist->at(qidx)->second;
    auto& q     = qlist->at(qidx)->first;

    //Crear directorio de logs
    fs::path logfile = fs::path(log_dir) / ("worker_" + std::to_string(worker_id) + ".log");

    std::ofstream fout(logfile, std::ios::out);
    if (!fout.is_open()) {
        std::cerr << "No pude abrir el archivo de log para worker " << worker_id
                  << ": " << logfile << "\n";
        return;
    }
    const std::string message = "WORKER";

    while (true) {
        if (qinfo.finished && qinfo.size == 0) {
            break;
        }

        auto job_opt = q.pop();
        if (!job_opt.has_value()) {
            break;
        }

        auto job = *job_opt;
        if (job.id == -1) {
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(job.worktime));
        auto last_size = qinfo.size.fetch_sub(1, std::memory_order_relaxed);
        auto last_weight = qinfo.weight.fetch_sub(job.worktime, std::memory_order_relaxed);
        qinfo.departures.fetch_add(1, std::memory_order_relaxed);

        log_event(fout, "POP", job.id, job.worktime, qinfo.id, worker_id,last_size, last_weight,message);
    }
    fout.flush();
}

int powerof2(auto &qlist){
    auto n = qlist.size();
    auto first = Random::randint(0, n-1);
    auto second = Random::randint(0, n-1);
    //if(first == second) return powerof2(qlist);
    int c_first = qlist.at(first)->second.size;
    int c_second = qlist.at(second)->second.size;
    return c_first >= c_second ? second : first;
}


int get_jsq_idx_rand(auto &qlist){
    auto n = qlist.size();
    int start = Random::randint(0, n-1);
    int qidx = start;
    int jobs_remaining = qlist.at(qidx)->second.size;
    unsigned int x = 0;
    while( (++x) < n ){
        auto subidx = (start + x) % n;
        auto& [squeue, qinfo] = *(qlist.at(subidx));
        if( jobs_remaining > qinfo.size){
            jobs_remaining = qinfo.size;
            qidx=subidx;
        }
    }

    return qidx;
}

int get_jsq_idx(auto& qlist){
    auto n = qlist.size();
    int qidx = 0;
    int jobs_remaining = qlist.at(qidx)->second.size;

    for(size_t i=1; i<qlist.size(); i++){
        auto& [squeue, qinfo] = *(qlist.at(i));
        if( jobs_remaining > qinfo.size){
            jobs_remaining = qinfo.size;
            qidx=i;
        }
    }
    return qidx;
}

int main(int argc, char **argv){
    //Config
    std::string config_path = "simulation_config.json";
    std::string folder_name = "default";
    START_TIME = now_us();

    if(argc > 1) folder_name = std::string(argv[1]);
    if(argc > 2) config_path = std::string(argv[2]);

    SimulationConfig config;
    try {
        config = load_simulation_config(config_path);
    } catch (const std::exception& e) {
        std::cerr << "No pude cargar " << config_path << ": " << e.what() << std::endl;
        return 1;
    }

    if(argc > 3) config.algorithm = parse_algorithm(std::string(argv[3]));

    algorithm = config.algorithm;
    auto seed = config.seed;
    auto nqueues = config.nqueues;
    auto max_queue_size = config.max_queue_size;
    auto nworkers = config.nworkers;
    auto njobs = config.njobs;
    auto nsimulations = config.nsimulations;
    auto simulation_seconds = config.simulation_seconds;
    auto arrival_interval_us = config.arrival_interval_us;
    auto min_w = config.min_w;
    auto max_w = config.max_w;

    //auto nthreads = 1;

    if( nworkers <= 0){
        std::cout << "nworkers must be greater than 0" << std::endl;
        return 1;
    }

    if( nqueues <= 0){
        std::cout << "nqueues must be greater than 0" << std::endl;
        return 1;
    }

    if( simulation_seconds <= 0.0 && nsimulations <= 0){
        std::cout << "simulation_seconds or nsimulations must be greater than 0" << std::endl;
        return 1;
    }


    std::cout << "program: " << *argv << std::endl;
    std::cout << "n_args: " << argc << std::endl;
    std::cout << "config: " << config_path << std::endl;
    std::cout << "seed: " << seed << std::endl;
    std::cout << "algorithm: " << algorithm_name(algorithm) << " (" << algorithm << ")" << std::endl;
    std::cout << "nqueues: " << nqueues << std::endl;
    std::cout << "max_queue_size: " << max_queue_size << std::endl;
    std::cout << "nworkers: " << nworkers << std::endl;
    std::cout << "njobs: " << njobs << std::endl;
    std::cout << "nsimulations: " << nsimulations << std::endl;
    std::cout << "simulation_seconds: " << simulation_seconds << std::endl;
    std::cout << "arrival_interval_us: " << arrival_interval_us << std::endl;


    fs::create_directories(folder_name);
    fs::copy_file(
        config_path,
        fs::path(folder_name) / "config.json",
        fs::copy_options::overwrite_existing
    );
    Random::seed(seed);

    //Initialize queues
    {
        QueueList qlist;
        std::vector<std::jthread> wlist;
        int worker_idx = 0;

        for(auto i=0; i<nqueues; i++){
            qlist.push_back(std::make_unique<JobQueue>());
            auto& [squeue, qinfo] = *(qlist.at(i));
            qinfo.id = i;
        }

        BayesianQueueSelector bayesian_selector;
        if (!bayesian_selector.load(config_path)) {
            std::cerr << "No pude cargar la config bayesiana desde " << config_path << std::endl;
            return 1;
        }

        for (std::size_t i = 0; i < qlist.size(); ++i) {
            bayesian_selector.add_queue(
                "queue_" + std::to_string(i),
                make_queue_status(qlist, i)
            );
        }

        for(auto i=0; i<nqueues; i++){
            //Inicializar workers
            for(int j=0; j<nworkers; j++){
                wlist.emplace_back(simple_worker, worker_idx, i, &qlist, folder_name);
                std::cout << "Worker " << worker_idx << " initiated." << std::endl;
                worker_idx++;
            }
        }

        //Start pushing jobs

        //Crear directorio de logs
        fs::path logfile = fs::path(folder_name) / (std::string("JSQ_simulation") + ".log");

        std::ofstream fout(logfile, std::ios::out);
        if (!fout.is_open()) {
            std::cerr << "No pude abrir el archivo de log para JSQ_simulation " << ": " << logfile << "\n";
            return -1;
        }

        std::string message = algorithm_name(algorithm);
        int jobs_dispatched = 0;
        const auto dispatch_start = std::chrono::steady_clock::now();
        auto next_arrival = dispatch_start;

        while (true) {
            if (simulation_seconds > 0.0) {
                const auto elapsed = std::chrono::duration<double>(
                    std::chrono::steady_clock::now() - dispatch_start
                ).count();

                if (elapsed >= simulation_seconds) {
                    break;
                }
            } else if (jobs_dispatched >= nsimulations) {
                break;
            }

            Job current_job = Job::create(jobs_dispatched, Random::randint(min_w, max_w));

            int qidx = 0;
            if (algorithm == JSQ) {
                qidx = get_jsq_idx(qlist);
            } else if (algorithm == LSL) {
                qidx = get_lsl_idx(qlist);
            } else if (algorithm == POWER_OF_2) {
                qidx = powerof2(qlist);
            } else {
                auto selected_qidx = bayesian_selector.choose_queue(
                    Priority::MEDIUM,
                    [&qlist](std::size_t sampled_idx, const QueueStatus&) {
                        return make_queue_status(qlist, sampled_idx);
                    }
                );

                qidx = selected_qidx.has_value()
                    ? static_cast<int>(*selected_qidx)
                    : get_jsq_idx_rand(qlist);
            }

            auto& [q,qinfo] = *(qlist.at(qidx));
            q.push(current_job);
            auto qsize = qinfo.size.fetch_add(1, std::memory_order_relaxed);
            auto qweight = qinfo.weight.fetch_add(current_job.worktime, std::memory_order_relaxed);
            qinfo.arrivals.fetch_add(1, std::memory_order_relaxed);
            log_event(fout, "PUSH", current_job.id, current_job.worktime, qinfo.id, -1, qsize, qweight,message);
            jobs_dispatched++;

            if (arrival_interval_us > 0) {
                next_arrival += std::chrono::microseconds(arrival_interval_us);
                std::this_thread::sleep_until(next_arrival);
            }
        }

        for(auto& i: qlist){
            i->second.finished=true;
            auto dropped_jobs = i->first.close_and_drain();
            int dropped_count = 0;
            int dropped_weight = 0;

            for (const auto& dropped_job : dropped_jobs) {
                if (dropped_job.id >= 0) {
                    dropped_count++;
                    dropped_weight += dropped_job.worktime;
                }
            }

            if (dropped_count > 0) {
                i->second.size.fetch_sub(dropped_count, std::memory_order_relaxed);
                i->second.weight.fetch_sub(dropped_weight, std::memory_order_relaxed);
            }
        }
        fout.flush();
        std::cout << "jobs_dispatched: " << jobs_dispatched << std::endl;

    }


}
