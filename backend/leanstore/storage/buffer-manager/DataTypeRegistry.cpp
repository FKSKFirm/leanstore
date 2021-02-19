#include "DataTypeRegistry.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
DataTypeRegistry DataTypeRegistry::global_dt_registry;
// -------------------------------------------------------------------------------------
void DataTypeRegistry::iterateChildrenSwips(DTID dtid, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback)
{
   auto dt_meta = dt_instances_ht[dtid];
   dt_types_ht[std::get<0>(dt_meta)].iterate_children(std::get<1>(dt_meta), bf, callback);
}
// -------------------------------------------------------------------------------------
ParentSwipHandler DataTypeRegistry::findParent(DTID dtid, BufferFrame& bf)
{
   auto dt_meta = dt_instances_ht[dtid];
   auto name = std::get<2>(dt_meta);
   return dt_types_ht[std::get<0>(dt_meta)].find_parent(std::get<1>(dt_meta), bf);
}
// -------------------------------------------------------------------------------------
bool DataTypeRegistry::checkSpaceUtilization(DTID dtid, BufferFrame& bf, OptimisticGuard& guard, ParentSwipHandler& parent_handler)
{
   auto dt_meta = dt_instances_ht[dtid];
   return dt_types_ht[std::get<0>(dt_meta)].check_space_utilization(std::get<1>(dt_meta), bf, guard, parent_handler);
}
// -------------------------------------------------------------------------------------
// Datastructures management
// -------------------------------------------------------------------------------------
void DataTypeRegistry::registerDatastructureType(DTType type, DataTypeRegistry::DTMeta dt_meta)
{
   std::unique_lock guard(mutex);
   dt_types_ht[type] = dt_meta;
}
// -------------------------------------------------------------------------------------
DTID DataTypeRegistry::registerDatastructureInstance(DTType type, void* root_object, string name)
{
   std::unique_lock guard(mutex);
   DTID new_instance_id = instances_counter++;
   dt_instances_ht.insert({new_instance_id, {type, root_object, name}});
   return new_instance_id;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
