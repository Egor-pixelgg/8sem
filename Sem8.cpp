#include <iostream>
#include <queue>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <random>
#include <algorithm>
#include <execution>
#include <numeric>
#include <fstream>
#include <map>
#include <iomanip>

std::mutex cout_mutex;

struct Telemetry {
    int sensor_id;
    double value;
};

struct SensorResult {
    double sum = 0.0;
    double max = 0.0;
    int count = 0;
};

class Dispatcher {
private:
    std::queue<std::vector<Telemetry>> buffer;
    std::mutex mtx;
    std::condition_variable cv;
    bool finished = false;

public:
    void push(std::vector<Telemetry>& data) {
        std::lock_guard<std::mutex> lock(mtx);
        buffer.push(std::move(data));
        cv.notify_one();
    }

    bool pop(std::vector<Telemetry>& data) {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [this] { return !buffer.empty() || finished; });
        if (buffer.empty()) return false;
        data = std::move(buffer.front());
        buffer.pop();
        return true;
    }

    void finish() {
        std::lock_guard<std::mutex> lock(mtx);
        finished = true;
        cv.notify_all();
    }
};

std::map<int, SensorResult> global_results;
std::mutex results_mutex;
std::atomic<int> packets_processed{ 0 };
std::atomic<int> finished_workers{ 0 };

void sensor_input(int start_id, int end_id, Dispatcher& dispatcher) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<double> dist(10.0, 100.0);

    std::vector<Telemetry> sensor_batch;

    for (int sid = start_id; sid < end_id; ++sid) {
        for (int sample = 0; sample < 5; ++sample) {
            Telemetry t;
            t.sensor_id = sid;
            t.value = dist(gen);
            sensor_batch.push_back(t);
            packets_processed++;
        }
    }

    dispatcher.push(sensor_batch);
}

void result_aggregator(Dispatcher& dispatcher, std::atomic<int>& active_workers) {
    std::vector<Telemetry> packet;

    while (active_workers.load() > 0) {
        if (dispatcher.pop(packet)) {
            std::map<int, SensorResult> local_results;

            for (const auto& t : packet) {
                local_results[t.sensor_id].sum += t.value;
                local_results[t.sensor_id].max = std::max(local_results[t.sensor_id].max, t.value);
                local_results[t.sensor_id].count++;
            }

            std::lock_guard<std::mutex> lock(results_mutex);
            for (const auto& [id, res] : local_results) {
                global_results[id].sum += res.sum;
                global_results[id].max = std::max(global_results[id].max, res.max);
                global_results[id].count += res.count;
            }
        }
    }
}

void storage_writer(const std::string& filename) {
    std::ofstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Ошибка создания файла!" << std::endl;
        return;
    }

    file << "sensor_id,sum,max,count\n";
    for (const auto& [id, res] : global_results) {
        file << id << ","
            << std::fixed << std::setprecision(2) << res.sum << ","
            << res.max << ","
            << res.count << "\n";
    }

    std::cout << "\nРезультаты сохранены в " << filename << std::endl;
}

int main() {
    setlocale(LC_ALL, "Russian");

    const int NUM_SENSORS = 100;
    const int INPUT_THREADS = 4;
    const int PROCESSING_THREADS = 4;

    Dispatcher dispatcher;
    std::atomic<int> active_workers = PROCESSING_THREADS;

    std::vector<std::thread> input_threads;
    int sensors_per_thread = NUM_SENSORS / INPUT_THREADS;

    for (int i = 0; i < INPUT_THREADS; ++i) {
        int start = i * sensors_per_thread;
        int end = (i == INPUT_THREADS - 1) ? NUM_SENSORS : start + sensors_per_thread;
        input_threads.emplace_back(sensor_input, start, end, std::ref(dispatcher));
    }

    std::vector<std::thread> processing_threads;
    for (int i = 0; i < PROCESSING_THREADS; ++i) {
        processing_threads.emplace_back([&] {
            std::vector<Telemetry> packet;

            while (dispatcher.pop(packet)) {
                std::sort(std::execution::par, packet.begin(), packet.end(),
                    [](const Telemetry& a, const Telemetry& b) {
                        return a.value < b.value;
                    });

                double sum = std::transform_reduce(std::execution::par_unseq,
                    packet.begin(), packet.end(), 0.0, std::plus<>(),
                    [](const Telemetry& t) { return t.value; });

                std::vector<double> scan(packet.size());
                std::transform_inclusive_scan(std::execution::par,
                    packet.begin(), packet.end(), scan.begin(),
                    std::plus<double>(),
                    [](const Telemetry& t) { return t.value; });

                double local_max = packet.back().value;

                {
                    std::lock_guard<std::mutex> lock(cout_mutex);
                    std::cout << "Обработан пакет. Сумма: "
                        << std::fixed << std::setprecision(2) << sum
                        << ", Максимум: " << std::fixed << std::setprecision(4) << local_max
                        << std::endl;
                }

                dispatcher.push(packet);
            }
            active_workers--;
            });
    }

    std::thread aggregator(result_aggregator, std::ref(dispatcher), std::ref(active_workers));

    for (auto& t : input_threads) {
        t.join();
    }

    dispatcher.finish();

    for (auto& t : processing_threads) {
        t.join();
    }

    aggregator.join();

    storage_writer("telemetry_results.csv");

    std::cout << "Обработано пакетов: " << packets_processed.load() << std::endl;
    std::cout << "Обработано сенсоров: " << global_results.size() << std::endl;

    return 0;
}
