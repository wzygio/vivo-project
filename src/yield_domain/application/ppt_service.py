# src/vivo_project/services/ppt_service.py
import os, logging
import shutil
import comtypes.client
import comtypes
import glob
import streamlit as st
from pathlib import Path

# [Refactor] 移除全局 PROJECT_ROOT

class PPTService:
    def __init__(self, output_dir_name: str, project_root: Path):
        self.project_root = project_root
        self.output_dir = self.project_root / output_dir_name

    def convert_to_images(self, ppt_relative_path: str):
        """
        将 PPT 转换为图片
        :param ppt_relative_path: 相对于项目根目录的路径，如 'resources/example.pptx'
        """
        # --- 1. 线程初始化 ---
        try:
            comtypes.CoInitialize() 
        except Exception:
            pass

        # 1. 构建 PPT 的绝对路径
        ppt_path = self.project_root / ppt_relative_path

        logging.info(f"PPT 输入路径 (绝对): {ppt_path}")
        logging.info(f"图片输出路径 (绝对): {self.output_dir}")

        # 2. 校验文件是否存在
        if not ppt_path.exists():
            err_msg = f"错误：找不到 PPT 文件 -> {ppt_path}"
            logging.error(err_msg)
            st.error(err_msg)
            return False

        # 3. 清理并重建输出目录
        if self.output_dir.exists():
            try:
                shutil.rmtree(self.output_dir)
            except Exception as e:
                logging.warning(f"清理旧文件夹失败 (可能是文件被占用): {e}")
        
        if not self.output_dir.exists():
            self.output_dir.mkdir(parents=True, exist_ok=True)

        # 4. 调用 COM 接口
        powerpoint = None
        presentation = None
        try:
            # 初始化 PowerPoint
            powerpoint = comtypes.client.CreateObject("PowerPoint.Application")
            powerpoint.Visible = 1 
            
            # 打开文件 (COM 需要字符串绝对路径)
            presentation = powerpoint.Presentations.Open(str(ppt_path.resolve()))
            
            # 导出图片
            for i, slide in enumerate(presentation.Slides, start=1):
                image_name = f"slide_{i:02d}.jpg"
                image_path = self.output_dir / image_name
                # 导出高清图
                slide.Export(str(image_path), "JPG", 2560, 1440)
                logging.info(f"已导出: {image_name}")

            return True

        except Exception as e:
            err_msg = f"PPT 转换失败: {e}"
            logging.error(err_msg)
            st.error(err_msg)
            return False
            
        finally:
            # 5. 安全关闭
            if presentation:
                try:
                    presentation.Close()
                except: pass
            if powerpoint:
                try:
                    powerpoint.Quit()
                except: pass

            try:
                comtypes.CoUninitialize()
            except:
                pass

    def get_images(self):
        """获取已生成的图片列表"""
        if not self.output_dir.exists():
            return []
        
        search_path = os.path.join(str(self.output_dir), "*.jpg")
        images = glob.glob(search_path)
        images.sort()
        return images