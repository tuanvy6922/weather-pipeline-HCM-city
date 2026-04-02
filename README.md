# Weather Data Pipeline - Ho Chi Minh City

## Giới thiệu
Pipeline tự động thu thập, xử lý và trực quan hóa dữ liệu thời tiết TP.HCM theo thời gian thực sử dụng kiến trúc Medallion.

## Kiến trúc hệ thống

OpenWeatherMap API → Apache Airflow → PostgreSQL → Power BI


## Công nghệ sử dụng

- **Python** - ETL scripting
- **Apache Airflow** - Pipeline orchestration
- **Docker** - Containerization
- **PostgreSQL** - Data warehouse
- **Power BI** - Data visualization

## Kiến trúc dữ liệu

### Medallion Architecture
- **Bronze** - Raw data từ API (`raw_weather`)
- **Silver** - Cleaned data (`silver_weather` View)
- **Gold** - Star Schema (`fact_weather` + dim tables)

### Star Schema
- `fact_weather` - Bảng fact chính
- `dim_date` - Chiều thời gian
- `dim_location` - Chiều địa điểm
- `dim_condition` - Chiều điều kiện thời tiết
- `dim_time_of_day` - Chiều buổi trong ngày

## Pipeline

DAG `weather_etl` chạy tự động mỗi giờ gồm 8 task:

1. `extract` - Gọi OpenWeatherMap API
2. `transform` - Xử lý dữ liệu thô
3. `load_raw` - Load vào Bronze layer
4. `load_dim_date` - Cập nhật dim_date
5. `load_dim_location` - Cập nhật dim_location
6. `load_dim_condition` - Cập nhật dim_condition
7. `load_dim_time` - Cập nhật dim_time_of_day
8. `load_fact` - Load vào fact_weather

## Cài đặt

### Yêu cầu
- Docker Desktop
- PostgreSQL
- Python 3.10
- Power BI Desktop

### Chạy dự án

1. Clone repository
```bash
git clone 
cd weather-pipeline


2. Khởi động Airflow
```bash
docker compose up -d


3. Tạo database trong PostgreSQL
```sql
CREATE DATABASE weather_db;


4. Truy cập Airflow UI

http://localhost:8080
Username: airflow
Password: airflow


5. Bật DAG `weather_etl` và trigger

## Dashboard Power BI

- Nhiệt độ trung bình theo giờ
- Độ ẩm trung bình theo giờ
- Phân loại thời tiết
- Tốc độ gió theo buổi
- Tốc độ gió theo giờ

## Tác giả

- **Họ tên:** Lê Tuấn Dĩ
- **Email:** tuandi2296@gmail.com
- **LinkedIn:** https://www.linkedin.com/in/d%C4%A9-l%C3%AA-3805a82b4/