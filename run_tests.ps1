# Скрипт для запуска тестов PySpark
# Устанавливает необходимые переменные окружения

# Установка переменных окружения для PySpark
$env:PYSPARK_PYTHON = "C:\Users\olegm\AppData\Local\Programs\Python\Python311\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "C:\Users\olegm\AppData\Local\Programs\Python\Python311\python.exe"

Write-Host "Запуск тестов PySpark..." -ForegroundColor Green
Write-Host "PYSPARK_PYTHON = $env:PYSPARK_PYTHON" -ForegroundColor Cyan
Write-Host "PYSPARK_DRIVER_PYTHON = $env:PYSPARK_DRIVER_PYTHON" -ForegroundColor Cyan
Write-Host ""

# Запуск тестов
pytest test_product_categories.py -v

