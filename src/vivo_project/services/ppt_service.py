# src/vivo_project/services/ppt_service.py
import os
import shutil
import comtypes.client
import glob
from vivo_project.config import PROJECT_ROOT

class PPTService:
    def __init__(self, output_dir):
        self.output_dir = output_dir

    def convert_to_images(self, ppt_path, resolution=(2560, 1440)):
        """将 PPT 转换为高清图片"""
        abs_input_path = os.path.abspath(PROJECT_ROOT / ppt_path)
        abs_output_dir = os.path.abspath(PROJECT_ROOT / self.output_dir)
        
        # 清理逻辑
        if os.path.exists(abs_output_dir):
            shutil.rmtree(abs_output_dir)
        os.makedirs(abs_output_dir)

        # 转换逻辑
        powerpoint = None
        presentation = None

        try:
            powerpoint = comtypes.client.CreateObject("PowerPoint.Application")
            powerpoint.Visible = 1 
            presentation = powerpoint.Presentations.Open(abs_input_path)

            for i, slide in enumerate(presentation.Slides, start=1):
                image_name = f"slide_{i:02d}.jpg"
                image_path = os.path.join(abs_output_dir, image_name)
                
                slide.Export(image_path, "JPG", resolution)
                
                print(f"已导出高清图片: {image_name}")

            return True
        except Exception as e:
            print(f"转换出错: {e}")
            return False
        finally:
            if presentation:
                presentation.Close()
            if powerpoint:
                powerpoint.Quit()

    def get_images(self):
        """获取已生成的图片列表"""
        search_path = os.path.join(self.output_dir, "*.jpg")
        images = glob.glob(search_path)
        images.sort()
        return images