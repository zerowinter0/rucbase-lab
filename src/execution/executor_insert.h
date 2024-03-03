#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class InsertExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Value> values_;
    RmFileHandle *fh_;
    std::string tab_name_;
    Rid rid_;
    SmManager *sm_manager_;

   public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        // Get record file handle
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override {
        // lab3 task3 Todo
        // Make record buffer
        // Insert into record file
        // Insert into index
        int tot_size=0;
        for(auto i:tab_.cols){
            tot_size+=i.len;
        }
        char *data=(char *)malloc(tot_size);
        for(int i=0;i<values_.size();i++){
            memcpy(data+tab_.cols[i].offset,values_[i].raw->data,values_[i].raw->size);
        }
        std::unique_ptr<RmRecord> rmRecord(new RmRecord(tot_size,data));
        auto ix_manager=sm_manager_->get_ix_manager();
        rid_=fh_->insert_record(data,context_);
        for(int i=0;i<tab_.cols.size();i++){
            if(tab_.cols[i].index){
                auto ix_file_handle=sm_manager_->ihs_.at(ix_manager->get_index_name(tab_name_,i)).get();
                ix_file_handle->insert_entry(values_[i].raw->data,rid_,nullptr);
            }
        }
        // lab3 task3 Todo end
        return rmRecord;
    }
    Rid &rid() override { return rid_; }
};