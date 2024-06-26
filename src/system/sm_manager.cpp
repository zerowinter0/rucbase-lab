#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

bool SmManager::is_dir(const std::string &db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

void SmManager::create_db(const std::string &db_name) {
    // lab3 task1 Todo
    // 利用*inx命令创建目录作为数据库
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    // Create a subdirectory for the database
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    // Create the system catalogs
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;
    
    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<
    delete new_db;

    // cd back to root dir
    if (chdir("..") < 0) {
        throw UnixError();
    }
    // lab3 task1 Todo End
}

void SmManager::drop_db(const std::string &db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

void SmManager::open_db(const std::string &db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    // cd to database dir
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }
    // Load meta
    // 打开一个名为DB_META_NAME的文件
    std::ifstream ifs(DB_META_NAME);
    // 将ofs打开的DB_META_NAME文件中的信息，按照定义好的operator>>操作符，读出到db_中
    ifs >> db_;  // 注意：此处重载了操作符>>
    // Open all record files & index files
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        // fhs_[tab.name] = rm_manager_->open_file(tab.name);
        fhs_.emplace(tab.name, rm_manager_->open_file(tab.name));
        for (size_t i = 0; i < tab.cols.size(); i++) {
            auto &col = tab.cols[i];
            if (col.index) {
                auto index_name = ix_manager_->get_index_name(tab.name, i);
                assert(ihs_.count(index_name) == 0);
                // ihs_[index_name] = ix_manager_->open_index(tab.name, i);
                ihs_.emplace(index_name, ix_manager_->open_index(tab.name, i));
            }
        }
    }
}

void SmManager::close_db() {
    // lab3 task1 Todo
    // 清理db_
    // 关闭rm_manager_ ix_manager_文件
    // 清理fhs_, ihs_

    std::ofstream ofs(DB_META_NAME);
    ofs<<db_;
    db_.name_.clear();
    db_.tabs_.clear();
    // Close all record files
    for (auto &entry : fhs_) {
        rm_manager_->close_file(entry.second.get());
    }
	fhs_.clear();
	// Close all index files
    for(auto &entry:ihs_){
        ix_manager_->close_index(entry.second.get());
    }
	ihs_.clear();
    if (chdir("..") < 0) {
        throw UnixError();
    }
    // lab3 task1 Todo End
}

void SmManager::show_tables(Context *context) {
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
    }
    printer.print_separator(context);
}

void SmManager::desc_table(const std::string &tab_name, Context *context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

void SmManager::create_table(const std::string &tab_name, const std::vector<ColDef> &col_defs, Context *context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));
}

void SmManager::drop_table(const std::string &tab_name, Context *context) {
    // lab3 task1 Todo
    // Find table index in db_ meta
    // Close & destroy record file
    // Close & destroy index file
    TabMeta &tab = db_.get_table(tab_name);
    context->lock_mgr_->LockExclusiveOnTable(context->txn_,fhs_[tab_name].get()->GetFd());
    rm_manager_->close_file(fhs_[tab_name].get());
    rm_manager_->destroy_file(tab_name);
    int cnt=0;
    for(auto col=tab.cols.begin();col!=tab.cols.end();col++){
        if(col->index){
            int col_idx=cnt;
            const std::string &col_name=ix_manager_->get_index_name(tab_name,col_idx);
            auto target=ihs_[col_name].get();
            ix_manager_->close_index(target);
            ix_manager_->destroy_index(tab_name,col_idx);
            ihs_.erase(col_name);
        }
        cnt++;
    }
    db_.tabs_.erase(tab_name);
    fhs_.erase(tab_name);
    // lab3 task1 Todo End
}

void SmManager::create_index(const std::string &tab_name, const std::string &col_name, Context *context) {
    TabMeta &tab = db_.get_table(tab_name);
    auto col = tab.get_col(col_name);
    if (col->index) {
        throw IndexExistsError(tab_name, col_name);
    }
    context->lock_mgr_->LockExclusiveOnTable(context->txn_,fhs_[tab_name].get()->GetFd());
    // Create index file
    int col_idx = col - tab.cols.begin();
    ix_manager_->create_index(tab_name, col_idx, col->type, col->len);  // 这里调用了
    // Open index file
    auto ih = ix_manager_->open_index(tab_name, col_idx);
    // Get record file handle
    auto file_handle = fhs_.at(tab_name).get();
    // Index all records into index
    for (RmScan rm_scan(file_handle); !rm_scan.is_end(); rm_scan.next()) {
        auto rec = file_handle->get_record(rm_scan.rid(), context);  // rid是record的存储位置，作为value插入到索引里
        const char *key = rec->data + col->offset;
        // record data里以各个属性的offset进行分隔，属性的长度为col len，record里面每个属性的数据作为key插入索引里
        ih->insert_entry(key, rm_scan.rid(), context->txn_);
    }
    // Store index handle
    auto index_name = ix_manager_->get_index_name(tab_name, col_idx);
    assert(ihs_.count(index_name) == 0);
    // ihs_[index_name] = std::move(ih);
    ihs_.emplace(index_name, std::move(ih));
    // Mark column index as created
    col->index = true;
}

void SmManager::drop_index(const std::string &tab_name, const std::string &col_name, Context *context) {
    TabMeta &tab = db_.tabs_[tab_name];
    auto col = tab.get_col(col_name);
    if (!col->index) {
        throw IndexNotFoundError(tab_name, col_name);
    }
    context->lock_mgr_->LockExclusiveOnTable(context->txn_,fhs_[tab_name].get()->GetFd());
    int col_idx = col - tab.cols.begin();
    auto index_name = ix_manager_->get_index_name(tab_name, col_idx);
    ix_manager_->close_index(ihs_.at(index_name).get());
    ix_manager_->destroy_index(tab_name, col_idx);
    ihs_.erase(index_name);
    col->index = false;
}
void SmManager::rollback_insert(const std::string &tab_name, const Rid &rid, Context *context){
        auto rm_handler=fhs_[tab_name].get();
        int cnt=0;
        auto tb=db_.get_table(tab_name);
        for(auto col:tb.cols){
            if(col.index){
                auto idx_handler=ihs_[get_ix_manager()->get_index_name(tab_name,cnt)].get();
                auto record=rm_handler->get_record(rid,context);
                auto key=record.get()->data+col.offset;
                idx_handler->delete_entry(key,context->txn_);
            }
            cnt++;
        }
        rm_handler->delete_record(rid,context);
}
void SmManager::rollback_delete(const std::string &tab_name, const RmRecord &record, Context *context){
        auto rm_handler=fhs_[tab_name].get();
        int cnt=0;
        auto rid=rm_handler->insert_record(record.data,context);
        auto tb=db_.get_table(tab_name);
        for(auto col:tb.cols){
            if(col.index){
                auto idx_handler=ihs_[get_ix_manager()->get_index_name(tab_name,cnt)].get();
                auto key=record.data+col.offset;
                idx_handler->insert_entry(key,rid,context->txn_);
            }
            cnt++;
        }
}

void SmManager::rollback_update(const std::string &tab_name, const Rid &rid, const RmRecord &record, Context *context)
{
        auto rm_handler=fhs_[tab_name].get();
        int cnt=0;

        auto pre_record=rm_handler->get_record(rid,context).get();

        auto tb=db_.get_table(tab_name);
        for(auto col:tb.cols){
            if(col.index){
                auto idx_handler=ihs_[get_ix_manager()->get_index_name(tab_name,cnt)].get();
                auto key=record.data+col.offset;
                auto pre_key=pre_record->data+col.offset;
                idx_handler->delete_entry(pre_key,context->txn_);
                idx_handler->insert_entry(key,rid,context->txn_);
            }
            cnt++;
        }
        rm_handler->update_record(rid,record.data,context);
}