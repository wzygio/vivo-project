import os
import logging
from typing import Dict, List

class FileManagerService:
    @staticmethod
    def get_classified_files(directory: str) -> Dict[str, List[str]]:
        """
        扫描目录并按规则分类文件
        返回格式: {'ledger': [], 'weekly': [], 'others': []}
        """
        if not os.path.exists(directory):
            logging.warning(f"目录不存在: {directory}")
            return {'ledger': [], 'weekly': [], 'others': []}

        # 支持的扩展名
        valid_exts = ('.xlsx', '.xls', '.pptx', '.ppt', '.pdf')
        
        classified = {
            'ledger': [], # 北极星台账类
            'weekly': [], # 北极星周报类
            'others': []  # 其他
        }

        files = [f for f in os.listdir(directory) if f.lower().endswith(valid_exts)]
        
        for f in files:
            # 简单的关键词匹配逻辑
            if "台账" in f:
                classified['ledger'].append(f)
            elif "周报" in f:
                classified['weekly'].append(f)
            else:
                classified['others'].append(f)
        
        # 为了美观，排个序
        for key in classified:
            classified[key].sort(reverse=True) # 通常按时间倒序比较好

        return classified

    @staticmethod
    def get_file_type(filename: str) -> str:
        """根据后缀判断类型标识"""
        ext = os.path.splitext(filename)[1].lower()
        if ext in ['.xlsx', '.xls']:
            return 'EXCEL'
        elif ext in ['.ppt', '.pptx']:
            return 'PPT'
        elif ext == '.pdf':
            return 'PDF'
        return 'UNKNOWN'