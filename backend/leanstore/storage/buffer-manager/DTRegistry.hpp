#pragma once
#include "BufferFrame.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <functional>
#include <mutex>
#include <tuple>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
using DTType = u8;
namespace storage
{
// -------------------------------------------------------------------------------------
struct ParentSwipHandler {
   Swip<BufferFrame>& swip;
   Guard parent_guard;
   BufferFrame* parent_bf;
   s32 pos = -2;  // meaning it is the root bf in the dt
   // -------------------------------------------------------------------------------------
   template <typename T>
   HybridPageGuard<T> getParentReadPageGuard()
   {
      return HybridPageGuard<T>(parent_guard, parent_bf);
   }
};
// -------------------------------------------------------------------------------------
struct DTRegistry {
   struct DTMeta {
      std::function<void(void*, BufferFrame&, std::function<bool(Swip<BufferFrame>&)>)> iterate_children;
      std::function<ParentSwipHandler(void*, BufferFrame&)> find_parent;
      std::function<bool(void*, BufferFrame&, OptimisticGuard&, ParentSwipHandler&)> check_space_utilization;
   };
   // -------------------------------------------------------------------------------------
   // TODO: synchronize properly
   std::mutex mutex;
   s64 instances_counter = 0;
   std::unordered_map<DTType, DTMeta> dt_types_ht;
   std::unordered_map<u64, std::tuple<DTType, void*, string>> dt_instances_ht;
   static DTRegistry global_dt_registry;
   // -------------------------------------------------------------------------------------
   void registerDatastructureType(DTType type, DTRegistry::DTMeta dt_meta);
   DTID registerDatastructureInstance(DTType type, void* root_object, string name);
   void registerDatastructureInstance(DTType type, void* root_object, string name, DTID dt_id);
   // -------------------------------------------------------------------------------------
   void iterateChildrenSwips(DTID dtid, BufferFrame&, std::function<bool(Swip<BufferFrame>&)>);
   ParentSwipHandler findParent(DTID dtid, BufferFrame&);
   bool checkSpaceUtilization(DTID dtid, BufferFrame&, OptimisticGuard&, ParentSwipHandler&);
};

// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
