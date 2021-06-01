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

//TODO: change adapter for one big LSM tree (foldRecord analog to RocksDB)

using namespace leanstore;
template <class Record>
struct LeanStoreAdapter {
   storage::KeyValueInterface* keyValueDataStore;
   std::map<std::string, Record> map;
   string name;
   int32_t id;

   LeanStoreAdapter()
   {
      // hack
   }
   LeanStoreAdapter(LeanStore& db, string name, int32_t id) : name(name), id(id)
   {
      if (FLAGS_allInOneKVStore) {
         if (FLAGS_lsm) {
            keyValueDataStore = &db.registerLsmTree("kvStore");
         } else {
            keyValueDataStore = &db.registerBTreeLL("kvStore");
         }
      }else {
         if (FLAGS_vw) {
            // removed
         } else if (FLAGS_vi) {
            // removed
         } else if (FLAGS_lsm) {
            keyValueDataStore = &db.registerLsmTree(name);
         } else {
            keyValueDataStore = &db.registerBTreeLL(name);
         }
      }
   }
   // -------------------------------------------------------------------------------------
   void printTreeHeight() { cout << name << " height = " << keyValueDataStore->getHeight() << endl; }
   // -------------------------------------------------------------------------------------
   template <class Fn>
   void scanDesc(const typename Record::Key& key, const Fn& fn)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(this->id)];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      if (FLAGS_allInOneKVStore) {
         folded_key_len = fold(folded_key, this->id) + Record::foldRecord(folded_key + sizeof(this->id), key);
      }
      keyValueDataStore->scanDesc(
          folded_key, folded_key_len,
          [&](const u8* key, [[maybe_unused]] u16 key_length, const u8* payload, [[maybe_unused]] u16 payload_length) {
             if (key_length != folded_key_len) {
                return false;
             }
             typename Record::Key typed_key;
             if (FLAGS_allInOneKVStore) {
                Record::unfoldRecord(reinterpret_cast<const u8*>(key + sizeof(this->id)), typed_key);
             }
             else {
                Record::unfoldRecord(key, typed_key);
             }
             const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
             return fn(typed_key, typed_payload);
          });
   }
   // -------------------------------------------------------------------------------------
   void insert(const typename Record::Key& rec_key, const Record& record)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(this->id)];
      u16 folded_key_len = Record::foldRecord(folded_key, rec_key);
      if (FLAGS_allInOneKVStore) {
         folded_key_len = fold(folded_key, this->id) + Record::foldRecord(folded_key + sizeof(this->id), rec_key);
      }
      const auto res = keyValueDataStore->insert(folded_key, folded_key_len, (u8*)(&record), sizeof(Record));
      ensure(res == OP_RESULT::OK || res == OP_RESULT::ABORT_TX);
      if (res == OP_RESULT::ABORT_TX) {
      }
   }

   template <class Fn>
   void lookup1(const typename Record::Key& key, const Fn& fn)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(this->id)];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      if (FLAGS_allInOneKVStore) {
         folded_key_len = fold(folded_key, this->id) + Record::foldRecord(folded_key + sizeof(this->id), key);
      }
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
      u8 folded_key[Record::maxFoldLength() + sizeof(id)];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      if (FLAGS_allInOneKVStore) {
         folded_key_len = fold(folded_key, this->id) + Record::foldRecord(folded_key + sizeof(this->id), key);
      }
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
      u8 folded_key[Record::maxFoldLength() + sizeof(this->id)];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      if (FLAGS_allInOneKVStore) {
         folded_key_len = fold(folded_key, this->id) + Record::foldRecord(folded_key + sizeof(this->id), key);
      }
      const auto res = keyValueDataStore->remove(folded_key, folded_key_len);
      if (res == OP_RESULT::ABORT_TX) {
      }
      return (res == OP_RESULT::OK);
   }
   // -------------------------------------------------------------------------------------
   template <class Fn>
   void scan(const typename Record::Key& key, const Fn& fn)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(this->id)];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      if (FLAGS_allInOneKVStore) {
         folded_key_len = fold(folded_key, this->id) + Record::foldRecord(folded_key + sizeof(this->id), key);
      }
      keyValueDataStore->scanAsc(
          folded_key, folded_key_len,
          [&](const u8* key, u16 key_length, const u8* payload, u16 payload_length) {
             if (key_length != folded_key_len) {
               return false;
            }
            static_cast<void>(payload_length);
            typename Record::Key typed_key;
            if (FLAGS_allInOneKVStore) {
               Record::unfoldRecord(reinterpret_cast<const u8*>(key + sizeof(this->id)), typed_key);
            }
            else {
               Record::unfoldRecord(key, typed_key);
            }
            const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
            return fn(typed_key, typed_payload);
          });
   }
   // -------------------------------------------------------------------------------------
   template <class Field>
   auto lookupField(const typename Record::Key& key, Field Record::*f)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(this->id)];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      if (FLAGS_allInOneKVStore) {
         folded_key_len = fold(folded_key, this->id) + Record::foldRecord(folded_key + sizeof(this->id), key);
      }
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
