# src/vivo_project/services/pdf_service.py
import os
import logging
import shutil
import glob
import fitz  # PyMuPDF
import streamlit as st
from vivo_project.config import PROJECT_ROOT

class PDFService:
    def __init__(self, output_dir):
        # 保持与 PPTService 一致的构造函数
        self.output_dir = os.path.join(PROJECT_ROOT, output_dir)

    def convert_to_images(self, pdf_relative_path):
        """
        将 PDF 转换为图片
        :param pdf_relative_path: 相对于项目根目录的路径，如 'resources/manual.pdf'
        """
        # 1. 构建绝对路径
        pdf_path = os.path.join(PROJECT_ROOT, pdf_relative_path)

        logging.info(f"PDF 输入路径: {pdf_path}")
        logging.info(f"图片输出路径: {self.output_dir}")

        # 2. 校验文件
        if not os.path.exists(pdf_path):
            err_msg = f"错误：找不到 PDF 文件 -> {pdf_path}"
            logging.error(err_msg)
            st.error(err_msg)
            return False

        # 3. 清理并重建输出目录 (与 PPTService 逻辑保持一致)
        if os.path.exists(self.output_dir):
            try:
                shutil.rmtree(self.output_dir)
            except Exception as e:
                logging.warning(f"清理旧文件夹失败: {e}")
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # 4. 核心转换逻辑 (使用 PyMuPDF)
        doc = None
        try:
            doc = fitz.open(pdf_path)
            total_pages = len(doc)
            logging.info(f"PDF 打开成功，共 {total_pages} 页")

            for i in range(len(doc)):
                page = doc[i]

                zoom = 2.0 
                mat = fitz.Matrix(zoom, zoom)
                
                pix = page.get_pixmap(matrix=mat)
                
                # 保存为图片 (使用 PNG 格式，文字边缘更清晰)
                image_name = f"page_{i+1:02d}.png"
                image_path = os.path.join(self.output_dir, image_name)
                
                pix.save(image_path)
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
        if not os.path.exists(self.output_dir):
            return []
        
        # 注意：这里支持 png 和 jpg，增强兼容性
        search_path_png = os.path.join(self.output_dir, "*.png")
        search_path_jpg = os.path.join(self.output_dir, "*.jpg")
        
        images = glob.glob(search_path_png) + glob.glob(search_path_jpg)
        images.sort()
        return images