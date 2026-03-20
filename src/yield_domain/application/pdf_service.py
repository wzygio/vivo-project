# src/vivo_project/services/pdf_service.py
import os
import logging
import shutil
import glob
import fitz  # PyMuPDF
import streamlit as st
from pathlib import Path

# [Refactor] 移除全局 PROJECT_ROOT 引用

class PDFService:
    def __init__(self, output_dir_name: str, project_root: Path):
        """
        初始化 PDF 服务
        :param output_dir_name: 输出目录的名称（相对于项目根目录），如 'data/pdf_images'
        :param project_root: 项目根目录 Path 对象
        """
        self.project_root = project_root
        # 保持与 PPTService 一致的构造函数
        self.output_dir = self.project_root / output_dir_name

    def convert_to_images(self, pdf_relative_path: str):
        """
        将 PDF 转换为图片
        :param pdf_relative_path: 相对于项目根目录的路径，如 'resources/manual.pdf'
        """
        # 1. 构建绝对路径
        pdf_path = self.project_root / pdf_relative_path

        logging.info(f"PDF 输入路径: {pdf_path}")
        logging.info(f"图片输出路径: {self.output_dir}")

        # 2. 校验文件
        if not pdf_path.exists():
            err_msg = f"错误：找不到 PDF 文件 -> {pdf_path}"
            logging.error(err_msg)
            st.error(err_msg)
            return False

        # 3. 清理并重建输出目录
        if self.output_dir.exists():
            try:
                shutil.rmtree(self.output_dir)
            except Exception as e:
                logging.warning(f"清理旧文件夹失败: {e}")
        
        if not self.output_dir.exists():
            self.output_dir.mkdir(parents=True, exist_ok=True)

        # 4. 核心转换逻辑 (使用 PyMuPDF)
        doc = None
        try:
            # fitz.open 需要字符串路径
            doc = fitz.open(str(pdf_path))  # type: ignore
            total_pages = len(doc)
            logging.info(f"PDF 打开成功，共 {total_pages} 页")

            for i in range(len(doc)):
                page = doc[i]

                zoom = 2.0 
                mat = fitz.Matrix(zoom, zoom)  # type: ignore
                
                pix = page.get_pixmap(matrix=mat)
                
                # 保存为图片
                image_name = f"page_{i+1:02d}.png"
                image_path = self.output_dir / image_name
                
                pix.save(str(image_path))
                logging.info(f"已导出: {image_name}")

            return True

        except Exception as e:
            err_msg = f"PDF 转换失败: {e}"
            logging.error(err_msg)
            st.error(err_msg)
            return False
        
        finally:
            # 5. 安全关闭
            if doc:
                try:
                    doc.close()
                except: pass

    def get_images(self):
        """获取已生成的图片列表"""
        if not self.output_dir.exists():
            return []
        
        # glob 需要字符串路径
        output_dir_str = str(self.output_dir)
        
        search_path_png = os.path.join(output_dir_str, "*.png")
        search_path_jpg = os.path.join(output_dir_str, "*.jpg")
        
        images = glob.glob(search_path_png) + glob.glob(search_path_jpg)
        images.sort()
        return images