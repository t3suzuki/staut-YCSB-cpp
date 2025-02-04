#include <cassert>
#include <iostream>
#include <vector>
#include <pthread.h>
#include <unistd.h>
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/slice_transform.h"

#define quote(x) q(x)
#define q(x) #x
static char FLAGS_db_path[] = quote(DB_PATH);


int main() {
  static rocksdb::DB* db = nullptr;
  static rocksdb::WriteOptions wo;
  
  rocksdb::Options db_options;
  db_options.allow_mmap_reads = false;
  db_options.allow_mmap_writes = false;
  
  db_options.use_direct_reads = true;
  db_options.use_direct_io_for_flush_and_compaction = true;
  
  //db_options.db_write_buffer_size = 0;  // disabled
  db_options.write_buffer_size = 1073741824;
  db_options.create_if_missing = true;
  //db_options.manual_wal_flush = true;
  //db_options.allow_concurrent_memtable_write = false;
  //db_options.compaction_readahead_size = 0;
  db_options.max_background_jobs = 1;
  db_options.level0_file_num_compaction_trigger = 8;
  db_options.level0_slowdown_writes_trigger = 0;
  db_options.level0_stop_writes_trigger = 0;
  db_options.target_file_size_base =    2147483648ULL;
  db_options.max_bytes_for_level_base = 0;
  //db_options.disable_auto_compactions = true;
  db_options.compression = rocksdb::CompressionType::kNoCompression;
  db_options.max_open_files = -1; // read index and filter all.

  db_options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(20));

  
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_size = 4 * 1024;
  table_options.block_align = 1;
  table_options.index_type = rocksdb::BlockBasedTableOptions::kHashSearch;
  table_options.filter_policy = rocksdb::BlockBasedTableOptions().filter_policy;
  db_options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  
  printf("DB_PATH = %s\n", FLAGS_db_path);
  
  rocksdb::Status s;
  s = rocksdb::DB::Open(db_options, FLAGS_db_path, &db);
  if (!s.ok())
    std::cerr << s.ToString() << std::endl;

  wo.disableWAL = true;
  wo.sync = false;

  printf("Compacting...\n");
  db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
  printf("Compaction Done!\n");

  db->Close();
}
