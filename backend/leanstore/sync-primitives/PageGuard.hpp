#pragma once
#include "Exceptions.hpp"
#include "Latch.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
// Objects of this class must be thread local !
// OptimisticPageGuard can hold the mutex. There are 3 locations where it can release it:
// 1- Destructor if not moved
// 2- Assign operator
// 3- kill()
template <typename T>
class ExclusivePageGuard;
template <typename T>
class SharedPageGuard;
template <typename T>
class HybridPageGuard
{
  public:
   BufferFrame* bufferFrame = nullptr;
   Guard guard;
   bool keep_alive = true;
   // -------------------------------------------------------------------------------------
   // Constructors
   HybridPageGuard() : bufferFrame(nullptr), guard(nullptr) { jumpmu_registerDestructor(); }  // use with caution
   HybridPageGuard(Guard& guard, BufferFrame* bf) : bufferFrame(bf), guard(std::move(guard)) { jumpmu_registerDestructor(); }
   // -------------------------------------------------------------------------------------
   HybridPageGuard(HybridPageGuard& other) = delete;   // Copy constructor
   HybridPageGuard(HybridPageGuard&& other) = delete;  // Move constructor
   // -------------------------------------------------------------------------------------
   // I: Allocate a new page
   HybridPageGuard(DTID dt_id, bool keep_alive = true)
       : bufferFrame(&BMC::global_bf->allocatePage()), guard(bufferFrame->header.latch, GUARD_STATE::EXCLUSIVE), keep_alive(keep_alive)
   {
      assert(BMC::global_bf != nullptr);
      bufferFrame->page.dt_id = dt_id;
      jumpmu_registerDestructor();
   }
   // -------------------------------------------------------------------------------------
   // I: Root case
   HybridPageGuard(BufferFrame* bf) : bufferFrame(bf), guard(bf->header.latch)
   {
      guard.toOptimisticSpin();
      jumpmu_registerDestructor();
   }
   // -------------------------------------------------------------------------------------
   // I: Lock coupling
   template <typename T2>
   HybridPageGuard(HybridPageGuard<T2>& p_guard, Swip<T>& swip, const LATCH_FALLBACK_MODE if_contended = LATCH_FALLBACK_MODE::SPIN)
       : bufferFrame(&BMC::global_bf->tryFastResolveSwip(p_guard.guard, swip.template cast<BufferFrame>())), guard(bufferFrame->header.latch)
   {
      if (if_contended == LATCH_FALLBACK_MODE::SPIN) {
         guard.toOptimisticSpin();
      } else if (if_contended == LATCH_FALLBACK_MODE::EXCLUSIVE) {
         guard.toOptimisticOrExclusive();
      } else if (if_contended == LATCH_FALLBACK_MODE::SHARED) {
         guard.toOptimisticOrShared();
      }
      //TODO: if JUMPMU_STACK_SIZE is too small it fails here
      jumpmu_registerDestructor();
      // -------------------------------------------------------------------------------------
      DEBUG_BLOCK()
      {
         [[maybe_unused]] DTID p_dt_id = p_guard.bufferFrame->page.dt_id, dt_id = bufferFrame->page.dt_id;
         p_guard.recheck();
         recheck();
         assert(p_dt_id == dt_id);
      }
      // -------------------------------------------------------------------------------------
      p_guard.recheck();
   }
   // I: Downgrade exclusive
   HybridPageGuard(ExclusivePageGuard<T>&&) = delete;
   HybridPageGuard& operator=(ExclusivePageGuard<T>&&)
   {
      guard.unlock();
      return *this;
   }
   // I: Downgrade shared
   HybridPageGuard(SharedPageGuard<T>&&) = delete;
   HybridPageGuard& operator=(SharedPageGuard<T>&&)
   {
      guard.unlock();
      return *this;
   }
   // -------------------------------------------------------------------------------------
   // Assignment operator
   constexpr HybridPageGuard& operator=(HybridPageGuard& other) = delete;
   template <typename T2>
   constexpr HybridPageGuard& operator=(HybridPageGuard<T2>&& other)
   {
      bufferFrame = other.bufferFrame;
      guard = std::move(other.guard);
      keep_alive = other.keep_alive;
      return *this;
   }
   // -------------------------------------------------------------------------------------
   inline void incrementGSN() { bufferFrame->page.GSN++; }
   // -------------------------------------------------------------------------------------
   inline bool hasFacedContention() { return guard.faced_contention; }
   inline void unlock() { guard.unlock(); }
   inline void recheck() { guard.recheck(); }
   // -------------------------------------------------------------------------------------
   inline T& ref() { return *reinterpret_cast<T*>(bufferFrame->page.dt); }
   inline T* ptr() { return reinterpret_cast<T*>(bufferFrame->page.dt); }
   inline Swip<T> swip() { return Swip<T>(bufferFrame); }
   inline T* operator->() { return reinterpret_cast<T*>(bufferFrame->page.dt); }
   // -------------------------------------------------------------------------------------
   // Use with caution!
   void toShared() { guard.toShared(); }
   void toExclusive() { guard.toExclusive(); }
   // -------------------------------------------------------------------------------------
   void reclaim()
   {
      BMC::global_bf->reclaimPage(*(bufferFrame));
      //cout << "Reclaimed page: " << bufferFrame << endl;
      guard.state = GUARD_STATE::MOVED;
   }
   // -------------------------------------------------------------------------------------
   jumpmu_defineCustomDestructor(HybridPageGuard)
       // -------------------------------------------------------------------------------------
       ~HybridPageGuard()
   {
      if (guard.state == GUARD_STATE::EXCLUSIVE) {
         if (!keep_alive) {
            reclaim();
         }
      }
      guard.unlock();
      jumpmu::clearLastDestructor();
   }
};
// -------------------------------------------------------------------------------------
template <typename T>
class ExclusivePageGuard
{
  private:
   HybridPageGuard<T>& ref_guard;

  public:
   // -------------------------------------------------------------------------------------
   // I: Upgrade
   ExclusivePageGuard(HybridPageGuard<T>&& o_guard) : ref_guard(o_guard)
   {
      ref_guard.guard.toExclusive();
      ref_guard.incrementGSN();
   }
   // -------------------------------------------------------------------------------------
   template <typename... Args>
   void init(Args&&... args)
   {
      new (ref_guard.bufferFrame->page.dt) T(std::forward<Args>(args)...);
   }
   // -------------------------------------------------------------------------------------
   void keepAlive() { ref_guard.keep_alive = true; }
   // -------------------------------------------------------------------------------------
   ~ExclusivePageGuard()
   {
      if (!ref_guard.keep_alive && ref_guard.guard.state == GUARD_STATE::EXCLUSIVE) {
         ref_guard.reclaim();
      } else {
         ref_guard.unlock();
      }
   }
   // -------------------------------------------------------------------------------------
   inline T& ref() { return *reinterpret_cast<T*>(ref_guard.bufferFrame->page.dt); }
   inline T* ptr() { return reinterpret_cast<T*>(ref_guard.bufferFrame->page.dt); }
   inline Swip<T> swip() { return Swip<T>(ref_guard.bufferFrame); }
   inline T* operator->() { return reinterpret_cast<T*>(ref_guard.bufferFrame->page.dt); }
   inline BufferFrame* getBufferFrame() { return ref_guard.bufferFrame; }
   inline void reclaim() { ref_guard.reclaim(); }
};
// -------------------------------------------------------------------------------------
template <typename T>
class SharedPageGuard
{
  public:
   HybridPageGuard<T>& ref_guard;
   // I: Upgrade
   SharedPageGuard(HybridPageGuard<T>&& h_guard) : ref_guard(h_guard) { ref_guard.toShared(); }
   // -------------------------------------------------------------------------------------
   ~SharedPageGuard() { ref_guard.unlock(); }
   // -------------------------------------------------------------------------------------
   inline T& ref() { return *reinterpret_cast<T*>(ref_guard.bufferFrame->page.dt); }
   inline T* ptr() { return reinterpret_cast<T*>(ref_guard.bufferFrame->page.dt); }
   inline Swip<T> swip() { return Swip<T>(ref_guard.bufferFrame); }
   inline T* operator->() { return reinterpret_cast<T*>(ref_guard.bufferFrame->page.dt); }
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
