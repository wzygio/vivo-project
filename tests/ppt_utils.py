import os
import comtypes.client
import glob
import shutil  # <--- 新增：用于删除文件夹

def ppt_to_images(input_ppt_path, output_folder):
    """
    将 PPT 的每一页转换为高清 JPG 图片，并在转换前清理旧图片
    """
    abs_input_path = os.path.abspath(input_ppt_path)
    abs_output_folder = os.path.abspath(output_folder)

    # --- [新增功能]：清理旧图片 ---
    if os.path.exists(abs_output_folder):
        try:
            # 直接删除整个文件夹及其内容
            shutil.rmtree(abs_output_folder)
            print(f"旧文件夹已清理: {abs_output_folder}")
        except Exception as e:
            print(f"清理文件夹失败: {e}")
    
    # 重新创建空文件夹
    os.makedirs(abs_output_folder)
    # ---------------------------

    powerpoint = None
    presentation = None

    try:
        powerpoint = comtypes.client.CreateObject("PowerPoint.Application")
        powerpoint.Visible = 1 
        presentation = powerpoint.Presentations.Open(abs_input_path)

        for i, slide in enumerate(presentation.Slides, start=1):
            image_name = f"slide_{i:02d}.jpg"
            image_path = os.path.join(abs_output_folder, image_name)
            
            # --- [关键修改]：指定高清分辨率 (宽度, 高度) ---
            # 2560x1440 适合大多数 2K 显示器，全屏效果极佳
            # 如果你的屏幕是 1080P，也可以改成 1920, 1080
            slide.Export(image_path, "JPG", 2560, 1440)
            
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

def get_sorted_images(folder_path):
    # 保持不变
    search_path = os.path.join(folder_path, "*.jpg")
    images = glob.glob(search_path)
    images.sort()
    return images