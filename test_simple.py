import pandas as pd
import qlib

if __name__ == '__main__':
    print("=== 超简单测试 ===")
    
    try:
        # 1. 测试数据读取
        print("1. 测试数据读取...")
        pred_score = pd.read_csv("C:/Users/ASUS/Downloads/预测数据_考虑相关系数.csv")
        print(f"   数据读取成功: {len(pred_score)} 行")
        print(f"   数据列: {list(pred_score.columns)}")
        print("   前3行数据:")
        print(pred_score.head(3))
        
        # 2. 测试QLib初始化
        print("\n2. 测试QLib初始化...")
        qlib.init(provider_uri="C:/Users/ASUS/Downloads/qlib_data", region="us")
        print("   QLib初始化成功")
        
        # 3. 测试数据处理
        print("\n3. 测试数据处理...")
        # 只取前10行进行测试
        small_data = pred_score.head(10).copy()
        small_data["datetime"] = pd.to_datetime(small_data["datetime"])
        small_data = small_data.set_index(["instrument", "datetime"]).sort_index()
        print(f"   处理后数据形状: {small_data.shape}")
        print("   处理后数据:")
        print(small_data)
        
        print("\n=== 基础测试全部通过 ===")
        
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
