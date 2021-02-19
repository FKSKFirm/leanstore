#pragma once
#include "BufferFrame.hpp"
#include "DTTypes.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <functional>
#include <tuple>
#include <unordered_map>
#include <mutex>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
struct ParentSwipHandler {
   Swip<BufferFrame>& swip;
   Guard parent_guard;
   BufferFrame* parent_bf;
   s32 pos = -2;  // meaning it is the root getBufferFrame in the dt
   // -------------------------------------------------------------------------------------
   template <typename T>
   HybridPageGuard<T> getParentReadPageGuard()
   {
      return HybridPageGuard<T>(parent_guard, parent_bf);
   }
};
// -------------------------------------------------------------------------------------
struct DataTypeRegistry {
   struct DTMeta {
      std::function<void(void*, BufferFrame&, std::function<bool(Swip<BufferFrame>&)>)> iterate_children;
      std::function<ParentSwipHandler(void*, BufferFrame&)> find_parent;
      std::function<bool(void*, BufferFrame&, OptimisticGuard&, ParentSwipHandler&)> check_space_utilization;
   };
   // -------------------------------------------------------------------------------------
   // TODO: Not syncrhonized
   std::mutex mutex;
   u64 instances_counter = 0;
   static DataTypeRegistry global_dt_registry;
   // -------------------------------------------------------------------------------------
   std::unordered_map<DTType, DTMeta> dt_types_ht;
   std::unordered_map<u64, std::tuple<DTType, void*, string>> dt_instances_ht;
   // -------------------------------------------------------------------------------------
   void registerDatastructureType(DTType type, DataTypeRegistry::DTMeta dt_meta);
   DTID registerDatastructureInstance(DTType type, void* root_object, string name);
   // -------------------------------------------------------------------------------------
   void iterateChildrenSwips(DTID dtid, BufferFrame&, std::function<bool(Swip<BufferFrame>&)>);
   ParentSwipHandler findParent(DTID dtid, BufferFrame&);
   bool checkSpaceUtilization(DTID dtid, BufferFrame&, OptimisticGuard&, ParentSwipHandler&);
};

// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
