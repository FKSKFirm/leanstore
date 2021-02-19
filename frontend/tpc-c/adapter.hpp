#pragma once
#include "types.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/LeanStore.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <map>
#include <string>

using namespace leanstore;
// TODO why and what is the template class Record?
template <class Record>
struct LeanStoreAdapter {
   // TODO maybe change keyValueDataStore to storageEngine
   storage::KeyValueInterface* keyValueDataStore;
   //TODO maybe add direct "storage::lsmtree::LSMTree* lsmTree;" for LSM Tree instead of new Interface
   // or change to storage::StorageEngineInterface
   std::map<std::string, Record> map;
   string name;
   LeanStoreAdapter()
   {
      // hack
   }
   LeanStoreAdapter(LeanStore& db, string name) : name(name)
   {
      //TODO: Register here the LSM-Tree or own BTree
      if (FLAGS_vw) {
         //removed
      } else if (FLAGS_vi) {
         //removed
      } else if (FLAGS_lsm) {
//         keyValueDataStore = &db.registerOwnBTree(name);
         //keyValueDataStore = &db.registerLSMTree(name);
      } else {
         keyValueDataStore = &db.registerBTreeLL(name);
      }
   }
   // -------------------------------------------------------------------------------------
   void printTreeHeight() { cout << name << " height = " << keyValueDataStore->getHeight() << endl; }
   // -------------------------------------------------------------------------------------
   template <class Fn>
   void scanDesc(const typename Record::Key& key, const Fn& fn, std::function<void()> undo)
   {
      // TODO what does the Record respective fold?
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      // TODO maybe change keyValueDataStore to storageEngine
      keyValueDataStore->scanDesc(
          folded_key, folded_key_len,
          [&](const u8* key, [[maybe_unused]] u16 key_length, const u8* payload, [[maybe_unused]] u16 payload_length) {
             if (key_length != folded_key_len) {
                return false;
             }
             typename Record::Key typed_key;
             Record::unfoldRecord(key, typed_key);
             const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
             return fn(typed_key, typed_payload);
          },
          undo);
   }
   // -------------------------------------------------------------------------------------
   void insert(const typename Record::Key& rec_key, const Record& record)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, rec_key);
      const auto res = keyValueDataStore->insert(folded_key, folded_key_len, (u8*)(&record), sizeof(Record));
      ensure(res == OP_RESULT::OK || res == OP_RESULT::ABORT_TX);
      if (res == OP_RESULT::ABORT_TX) {
      }
   }

   template <class Fn>
   void lookup1(const typename Record::Key& key, const Fn& fn)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      const auto res = keyValueDataStore->lookup(folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
         static_cast<void>(payload_length);
         const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
         assert(payload_length == sizeof(Record));
         fn(typed_payload);
      });
      ensure(res == OP_RESULT::OK);
   }

   template <class Fn>
   void update1(const typename Record::Key& key, const Fn& fn)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      const auto res = keyValueDataStore->updateSameSize(
          folded_key, folded_key_len,
          [&](u8* payload, u16 payload_length) {
             static_cast<void>(payload_length);
             assert(payload_length == sizeof(Record));
             Record& typed_payload = *reinterpret_cast<Record*>(payload);
             fn(typed_payload);
          });
      ensure(res != OP_RESULT::NOT_FOUND);
      if (res == OP_RESULT::ABORT_TX) {
      }
   }

   bool erase(const typename Record::Key& key)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      const auto res = keyValueDataStore->remove(folded_key, folded_key_len);
      if (res == OP_RESULT::ABORT_TX) {
      }
      return (res == OP_RESULT::OK);
   }
   // -------------------------------------------------------------------------------------
   template <class Fn>
   void scan(const typename Record::Key& key, const Fn& fn, std::function<void()> undo)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      keyValueDataStore->scanAsc(
          folded_key, folded_key_len,
          [&](const u8* key, u16 key_length, const u8* payload, u16 payload_length) {
             if (key_length != folded_key_len) {
                return false;
             }
             static_cast<void>(payload_length);
             typename Record::Key typed_key;
             Record::unfoldRecord(key, typed_key);
             const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
             return fn(typed_key, typed_payload);
          },
          undo);
   }
   // -------------------------------------------------------------------------------------
   template <class Field>
   auto lookupField(const typename Record::Key& key, Field Record::*f)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      Field local_f;
      const auto res = keyValueDataStore->lookup(folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
         static_cast<void>(payload_length);
         assert(payload_length == sizeof(Record));
         Record& typed_payload = *const_cast<Record*>(reinterpret_cast<const Record*>(payload));
         local_f = (typed_payload).*f;
      });
      ensure(res == OP_RESULT::OK);
      return local_f;
   }

   uint64_t count() { return keyValueDataStore->countEntries(); }
};
