#include "rm_file_handle.h"

/**
 * @brief 由Rid得到指向RmRecord的指针
 *
 * @param rid 指定记录所在的位置
 * @return std::unique_ptr<RmRecord>
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid &rid, Context *context) const {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）
    context->lock_mgr_->LockSharedOnRecord(context->txn_,rid,fd_);
    int page_no=rid.page_no;
    int slot_no=rid.slot_no;
    auto page_handle=fetch_page_handle(page_no);
    auto new_record=std::make_unique<RmRecord> (file_hdr_.record_size);
    memcpy(new_record->data,page_handle.get_slot(slot_no),file_hdr_.record_size);
    new_record->size=file_hdr_.record_size;
    return new_record;
}

/**
 * @brief 在该记录文件（RmFileHandle）中插入一条记录
 *
 * @param buf 要插入的数据的地址
 * @return Rid 插入记录的位置
 */
Rid RmFileHandle::insert_record(char *buf, Context *context) {
    // Todo:
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no
    auto page_handle=create_page_handle();
    int free_slot=Bitmap::first_bit(0,page_handle.bitmap,file_hdr_.num_records_per_page);
    memcpy(page_handle.get_slot(free_slot),buf,file_hdr_.record_size);
    Bitmap::set(page_handle.bitmap,free_slot);
    if(++page_handle.page_hdr->num_records==file_hdr_.num_records_per_page)file_hdr_.first_free_page_no=page_handle.page_hdr->next_free_page_no;
    return Rid{page_handle.page->GetPageId().page_no, free_slot};
}

/**
 * @brief 在该记录文件（RmFileHandle）中删除一条指定位置的记录
 *
 * @param rid 要删除的记录所在的指定位置
 */
void RmFileHandle::delete_record(const Rid &rid, Context *context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()
    context->lock_mgr_->LockExclusiveOnRecord(context->txn_,rid,fd_);
    int page_no=rid.page_no;
    int slot_no=rid.slot_no;
    auto page_handle=fetch_page_handle(page_no);
    if(slot_no>=file_hdr_.num_records_per_page||!Bitmap::is_set(page_handle.bitmap,slot_no))throw RecordNotFoundError(page_no,slot_no);
    Bitmap::reset(page_handle.bitmap,slot_no);
    if(page_handle.page_hdr->num_records--==file_hdr_.num_records_per_page)release_page_handle(page_handle);
}

/**
 * @brief 更新指定位置的记录
 *
 * @param rid 指定位置的记录
 * @param buf 新记录的数据的地址
 */
void RmFileHandle::update_record(const Rid &rid, char *buf, Context *context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新记录
    context->lock_mgr_->LockExclusiveOnRecord(context->txn_,rid,fd_);
    int page_no=rid.page_no;
    int slot_no=rid.slot_no;
    auto page_handle=fetch_page_handle(page_no);
    memcpy(page_handle.get_slot(slot_no),buf,file_hdr_.record_size);
}

/** -- 以下为辅助函数 -- */
/**
 * @brief 获取指定页面编号的page handle
 *
 * @param page_no 要获取的页面编号
 * @return RmPageHandle 返回给上层的page_handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // Todo:
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception
    PageId pageid={fd_,page_no};
    Page* page=buffer_pool_manager_->FetchPage(pageid);//fetchpage内部已Pin
    
    return RmPageHandle(&file_hdr_,page);
}

/**
 * @brief 创建一个新的page handle
 *
 * @return RmPageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // Todo:
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_
    PageId pageid;
    pageid.fd=fd_;
    Page* page=buffer_pool_manager_->NewPage(&pageid);
    if(page==nullptr)return RmPageHandle(&file_hdr_, nullptr);
    auto res=RmPageHandle(&file_hdr_,page);
    assert(file_hdr_.first_free_page_no==-1);
    res.page_hdr->next_free_page_no=file_hdr_.first_free_page_no;//page_hdr
    res.page_hdr->num_records=0;
    file_hdr_.first_free_page_no=pageid.page_no;//file_hdr_
    file_hdr_.num_pages++;
    res.page=page;//page
    Bitmap::init(res.bitmap,file_hdr_.bitmap_size);
    return res;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // Todo:
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层
    if(file_hdr_.first_free_page_no!=-1){
        return fetch_page_handle(file_hdr_.first_free_page_no);
    }
    else{
        auto res=create_new_page_handle();
        return res;
    }
}

/**
 * @brief 当page handle中的page从已满变成未满的时候调用
 *
 * @param page_handle
 * @note only used in delete_record()
 */
void RmFileHandle::release_page_handle(RmPageHandle &page_handle) {
    // Todo:
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no
    page_handle.page_hdr->next_free_page_no=file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no=page_handle.page->GetPageId().page_no;
}

/**
 * @brief 用于事务的rollback操作
 *
 * @param rid record的插入位置
 * @param buf record的内容
 */
void RmFileHandle::insert_record(const Rid &rid, char *buf) {
    if (rid.page_no < file_hdr_.num_pages) {
        create_new_page_handle();
    }
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    Bitmap::set(pageHandle.bitmap, rid.slot_no);
    pageHandle.page_hdr->num_records++;
    if (pageHandle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = pageHandle.page_hdr->next_free_page_no;
    }

    char *slot = pageHandle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);

    buffer_pool_manager_->UnpinPage(pageHandle.page->GetPageId(), true);
}
