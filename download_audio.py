import requests
def download_audio(url, filename):
    try:
        # Gửi yêu cầu HTTP GET
        response = requests.get(url, stream=True)
        response.raise_for_status() # Kiểm tra xem yêu cầu có thành công không
        # Mở file ở chế độ ghi nhị phân
        with open(filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        
        print(f"Tải xuống thành công: {filename}")
    except requests.exceptions.HTTPError as http_err:
        print(f"Lỗi HTTP: {http_err}")
    except Exception as err:
        print(f"Lỗi khác: {err}")
if __name__ == "__main__":
    url = "https://cdn.gty.org/sermons/High/81-167.mp3"
    filename = "81-167.mp3"
    download_audio(url, filename)