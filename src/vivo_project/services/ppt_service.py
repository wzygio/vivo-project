# src/vivo_project/services/ppt_service.py
import os, logging
import shutil
import comtypes.client
import comtypes
import glob
import streamlit as st
from vivo_project.config import PROJECT_ROOT

class PPTService:
    def __init__(self, output_dir):
        self.output_dir = os.path.join(PROJECT_ROOT, output_dir)

    def convert_to_images(self, ppt_relative_path):
        """
        将 PPT 转换为图片
        :param ppt_relative_path: 相对于项目根目录的路径，如 'resources/example.pptx'
        """
        # --- [核心修复] 1. 线程初始化：打卡报到 ---
        try:
            comtypes.CoInitialize() 
        except Exception:
            # 有时候如果已经初始化过，再次初始化可能会抛错，可以忽略
            pass

        # 1. 构建 PPT 的绝对路径
        ppt_path = os.path.join(PROJECT_ROOT, ppt_relative_path)

        logging.info(f"PPT 输入路径 (绝对): {ppt_path}")
        logging.info(f"图片输出路径 (绝对): {self.output_dir}")

        # 2. 校验文件是否存在
        if not os.path.exists(ppt_path):
            err_msg = f"错误：找不到 PPT 文件 -> {ppt_path}"
            logging.error(err_msg)
            st.error(err_msg) # 在界面上弹红框
            return False

        # 3. 清理并重建输出目录
        if os.path.exists(self.output_dir):
            try:
                shutil.rmtree(self.output_dir)
            except Exception as e:
                logging.warning(f"清理旧文件夹失败 (可能是文件被占用): {e}")
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # 4. 调用 COM 接口
        powerpoint = None
        presentation = None
        try:
            # 初始化 PowerPoint
            powerpoint = comtypes.client.CreateObject("PowerPoint.Application")
            powerpoint.Visible = 1 # 必须设为可见，否则某些版本会报错
            
            # 打开文件
            presentation = powerpoint.Presentations.Open(ppt_path)
            
            # 导出图片
            for i, slide in enumerate(presentation.Slides, start=1):
                image_name = f"slide_{i:02d}.jpg"
                image_path = os.path.join(self.output_dir, image_name)
                # 导出高清图 (2560x1440)
                slide.Export(image_path, "JPG", 2560, 1440)
                logging.info(f"已导出: {image_name}")

            return True

        except Exception as e:
            err_msg = f"PPT 转换失败: {e}"
            logging.error(err_msg)
            st.error(err_msg) # 关键：在界面上显示具体的报错原因
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

            # --- [核心修复] 2. 线程清理：下班打卡 ---
            try:
                comtypes.CoUninitialize()
            except:
                pass

    def get_images(self):
        """获取已生成的图片列表"""
        if not os.path.exists(self.output_dir):
            return []
        search_path = os.path.join(self.output_dir, "*.jpg")
        images = glob.glob(search_path)
        images.sort()
        return images