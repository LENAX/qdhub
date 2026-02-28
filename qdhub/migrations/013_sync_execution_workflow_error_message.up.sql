-- 当「工作流报失败但明细全成功」被纠正为成功时，保存工作流原始错误信息，供前端展示警告
ALTER TABLE sync_execution ADD COLUMN workflow_error_message TEXT;
