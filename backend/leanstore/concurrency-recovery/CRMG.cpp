#include "CRMG.hpp"

#include "leanstore/profiling/counters/CPUCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <mutex>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
WorkerThreadManager* WorkerThreadManager::global = nullptr;
// -------------------------------------------------------------------------------------
WorkerThreadManager::WorkerThreadManager()
{
   workers_count = FLAGS_worker_threads;
   ensure(workers_count < MAX_WORKER_THREADS);
   worker_threads.reserve(workers_count);
   for (u64 t_i = 0; t_i < workers_count; t_i++) {
      worker_threads.emplace_back([&, t_i]() {
         std::string thread_name("worker_" + std::to_string(t_i));
         pthread_setname_np(pthread_self(), thread_name.c_str());
         // -------------------------------------------------------------------------------------
         CPUCounters::registerThread(thread_name, false);
         // -------------------------------------------------------------------------------------
         workers[t_i] = new Worker(t_i, workers, workers_count);
         Worker::tls_ptr = workers[t_i];
         // -------------------------------------------------------------------------------------
         running_threads++;
         auto& meta = worker_threads_meta[t_i];
         while (keep_running) {
            std::unique_lock guard(meta.mutex);
            meta.cv.wait(guard, [&]() { return keep_running == false || meta.job_set; });
            if (!keep_running) {
               break;
            }
            meta.wt_ready = false;
            meta.job();
            meta.wt_ready = true;
            meta.job_done = true;
            meta.job_set = false;
            meta.cv.notify_one();
         }
         running_threads--;
      });
   }
   for (auto& t : worker_threads) {
      t.detach();
   }
   // -------------------------------------------------------------------------------------
   // Wait until all worker threads are initialized
   while (running_threads < workers_count) {
   }
   // -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
WorkerThreadManager::~WorkerThreadManager()
{
   keep_running = false;

   for (u64 t_i = 0; t_i < workers_count; t_i++) {
      worker_threads_meta[t_i].cv.notify_one();
   }
   while (running_threads) {
   }
   for (u64 t_i = 0; t_i < workers_count; t_i++) {
      delete workers[t_i];
   }
}
// -------------------------------------------------------------------------------------
void WorkerThreadManager::scheduleJobSync(u64 t_i, std::function<void()> job)
{
   ensure(t_i < workers_count);
   auto& meta = worker_threads_meta[t_i];
   std::unique_lock guard(meta.mutex);
   meta.cv.wait(guard, [&]() { return !meta.job_set && meta.wt_ready; });
   meta.job_set = true;
   meta.job_done = false;
   meta.job = job;
   guard.unlock();
   meta.cv.notify_one();
   guard.lock();
   meta.cv.wait(guard, [&]() { return meta.job_done; });
}
// -------------------------------------------------------------------------------------
void WorkerThreadManager::scheduleJobAsync(u64 t_i, std::function<void()> job)
{
   ensure(t_i < workers_count);
   auto& meta = worker_threads_meta[t_i];
   std::unique_lock guard(meta.mutex);
   meta.cv.wait(guard, [&]() { return !meta.job_set && meta.wt_ready; });
   meta.job_set = true;
   meta.job_done = false;
   meta.job = job;
   guard.unlock();
   meta.cv.notify_one();
}
// -------------------------------------------------------------------------------------
void WorkerThreadManager::joinAll()
{
   for (u32 t_i = 0; t_i < workers_count; t_i++) {
      auto& meta = worker_threads_meta[t_i];
      std::unique_lock guard(meta.mutex);
      meta.cv.wait(guard, [&]() { return meta.wt_ready && !meta.job_set; });
   }
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
