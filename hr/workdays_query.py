from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
from datetime import datetime
from auth import get_power_bi_token
from utils.get_inn import get_employee_inn  # Імпортуємо функцію для отримання INN
import requests
import logging

from utils.name_aliases import display_name


async def show_workdays_years(update: Update, context: CallbackContext) -> None:
    context.user_data['menu'] = 'workdays_years'

    current_year = datetime.now().year
    start_year = 2025
    end_year = max(current_year, 2025)

    years = [str(y) for y in range(start_year, end_year + 1)]

    keyboard = [[KeyboardButton(year)] for year in years]
    keyboard.append([KeyboardButton("Назад"), KeyboardButton("Головне меню")])

    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("🗓 Оберіть рік:", reply_markup=reply_markup)


async def show_workdays_months(update: Update, context: CallbackContext) -> None:
    selected_year = update.message.text
    context.user_data['selected_year'] = selected_year
    context.user_data['menu'] = 'workdays_months'

    months = [
        "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
        "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
    ]
    keyboard = [[KeyboardButton(month)] for month in months]
    keyboard.append([KeyboardButton("Назад"), KeyboardButton("Головне меню")])
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("📅 Оберіть місяць:", reply_markup=reply_markup)


async def show_workdays_details(update: Update, context: CallbackContext) -> None:
    selected_month = update.message.text
    context.user_data['selected_month'] = selected_month
    context.user_data['menu'] = 'workdays_details'

    employee_name = context.user_data.get('employee_name')
    year = context.user_data.get('selected_year')

    # Отримуємо INN співробітника
    tax_code = get_employee_inn(employee_name)
    
    if not tax_code:
        logging.warning(f"⚠️ Не вдалося знайти INN для {employee_name}. Використовуємо фільтрацію по імені.")
        filter_condition = f'workdays_by_employee[Employee] = "{employee_name}"'
    else:
        logging.info(f"✅ Знайдено INN для {employee_name}: {tax_code}")
        # Фільтруємо по tax_code
        filter_condition = f'CONVERTSTR(workdays_by_employee[tax_code], STRING) = "{tax_code}"'

    month_map = {
        "Січень": "01", "Лютий": "02", "Березень": "03", "Квітень": "04",
        "Травень": "05", "Червень": "06", "Липень": "07", "Серпень": "08",
        "Вересень": "09", "Жовтень": "10", "Листопад": "11", "Грудень": "12"
    }
    month_num = month_map.get(selected_month)
    if not month_num:
        await update.message.reply_text("⚠️ Невідомий місяць.")
        return

    token = get_power_bi_token()
    if not token:
        await update.message.reply_text("❌ Не вдалося отримати токен для Power BI.")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    dax_query = {
        "queries": [
            {
                "query": f"""
                    EVALUATE
                    SELECTCOLUMNS(
                        FILTER(
                            workdays_by_employee,
                            {filter_condition} &&
                            DATEVALUE(workdays_by_employee[Period]) = DATE({year}, {int(month_num)}, 1)
                        ),
                        "Period", workdays_by_employee[Period],
                        "TotalDays", workdays_by_employee[TotalDays],
                        "WeekendDays", workdays_by_employee[WeekendDays],
                        "HolidayDays", workdays_by_employee[HolidayDays],
                        "WorkDays", workdays_by_employee[WorkDays],
                        "LeaveWithoutPay", workdays_by_employee[LeaveWithoutPay],
                        "RegularVacationDays", workdays_by_employee[RegularVacationDays],
                        "SickLeaveDays", workdays_by_employee[SickLeaveDays],
                        "WorkedDays", workdays_by_employee[WorkedDays],
                        "Employee", workdays_by_employee[Employee],  # Додаємо для перевірки
                        "tax_code", workdays_by_employee[tax_code]   # Додаємо для дебагінга
                    )
                """
            }
        ],
        "serializerSettings": {"includeNulls": True}
    }

    power_bi_url = "https://api.powerbi.com/v1.0/myorg/datasets/8b80be15-7b31-49e4-bc85-8b37a0d98f1c/executeQueries"
    
    logging.info(f"📤 Запит до Power BI: {tax_code if tax_code else 'по імені'}")
    response = requests.post(power_bi_url, headers=headers, json=dax_query)

    logging.info(f"📥 Статус відповіді Power BI: {response.status_code}")
    logging.info(f"📄 Вміст відповіді: {response.text}")

    if response.status_code != 200:
        await update.message.reply_text("❌ Помилка при отриманні даних з Power BI.")
        return

    try:
        data = response.json()
        rows = data['results'][0]['tables'][0].get('rows', [])
    except Exception as e:
        logging.error(f"❌ Помилка при обробці відповіді: {e}")
        await update.message.reply_text("❌ Помилка при обробці даних.")
        return

    if not rows:
        # Спробуємо альтернативний метод пошуку, якщо не знайшли
        await try_alternative_search(update, context, employee_name, year, month_num, tax_code)
        return

    row = rows[0]

    # Отримуємо ім'я з відповіді (може відрізнятися від введеного)
    actual_employee_name = row.get('[Employee]', employee_name)
    nice_name = display_name(actual_employee_name)
    
    # Форматуємо дату періоду
    period = row.get('[Period]', '')
    if period and len(period) >= 10:
        period_str = period[:10]
    else:
        period_str = f"{year}-{month_num}"
    
    message = (
        f"📅 Період: {period_str}\n"
        f"👤 Працівник: {nice_name}\n"
        f"📊 Всього днів: {row.get('[TotalDays]', 0)}\n"
        f"📆 Робочі дні: {row.get('[WorkDays]', 0)}\n"
        f"🛌 Вихідні дні: {row.get('[WeekendDays]', 0)}\n"
        f"🎉 Святкові дні: {row.get('[HolidayDays]', 0)}\n"
        f"🚫 Відпустка за свій рахунок: {row.get('[LeaveWithoutPay]', 0)}\n"
        f"🏖 Звичайна відпустка: {row.get('[RegularVacationDays]', 0)}\n"
        f"🤒 Лікарняні: {row.get('[SickLeaveDays]', 0)}\n"
        f"✅ Відпрацьовано: {row.get('[WorkedDays]', 0)}\n"
    )

    await update.message.reply_text(message)

    # Додаємо кнопки після виводу
    keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("⬅️ Оберіть дію:", reply_markup=reply_markup)


async def try_alternative_search(update: Update, context: CallbackContext, 
                               employee_name: str, year: str, month_num: str, 
                               tax_code: str | None) -> None:
    """Альтернативний пошук даних"""
    token = get_power_bi_token()
    if not token:
        await update.message.reply_text("❌ Помилка отримання токена.")
        return
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    power_bi_url = "https://api.powerbi.com/v1.0/myorg/datasets/8b80be15-7b31-49e4-bc85-8b37a0d98f1c/executeQueries"
    
    # Спробуємо пошук тільки по tax_code без перевірки місяця
    if tax_code:
        dax_query = {
            "queries": [
                {
                    "query": f"""
                        EVALUATE
                        FILTER(
                            workdays_by_employee,
                            CONVERTSTR(workdays_by_employee[tax_code], STRING) = "{tax_code}"
                        )
                    """
                }
            ],
            "serializerSettings": {"includeNulls": True}
        }
        
        logging.info(f"🔍 Альтернативний пошук по tax_code: {tax_code}")
        response = requests.post(power_bi_url, headers=headers, json=dax_query)
        
        if response.status_code == 200:
            try:
                data = response.json()
                rows = data['results'][0]['tables'][0].get('rows', [])
                
                if rows:
                    # Знайшли дані, покажемо всі доступні місяці
                    months_found = []
                    for row in rows:
                        period = row.get('[Period]', '')
                        if period and str(year) in period:
                            months_found.append(period)
                    
                    if months_found:
                        months_list = "\n".join(months_found[:5])  # Перші 5
                        await update.message.reply_text(
                            f"ℹ️ Для {employee_name} знайдені дані за {year} рік:\n"
                            f"{months_list}\n\n"
                            f"Оберіть інший місяць або перевірте дані."
                        )
                        return
            except Exception as e:
                logging.error(f"❌ Помилка альтернативного пошуку: {e}")
    
    # Спробуємо пошук по імені
    dax_query = {
        "queries": [
            {
                "query": f"""
                    EVALUATE
                    SELECTCOLUMNS(
                        FILTER(
                            workdays_by_employee,
                            workdays_by_employee[Employee] = "{employee_name}" &&
                            YEAR(DATEVALUE(workdays_by_employee[Period])) = {year}
                        ),
                        "Period", workdays_by_employee[Period],
                        "WorkedDays", workdays_by_employee[WorkedDays]
                    )
                """
            }
        ],
        "serializerSettings": {"includeNulls": True}
    }
    
    response = requests.post(power_bi_url, headers=headers, json=dax_query)
    
    if response.status_code == 200:
        try:
            data = response.json()
            rows = data['results'][0]['tables'][0].get('rows', [])
            
            if rows:
                months_with_data = []
                for row in rows:
                    period = row.get('[Period]', '')
                    if period:
                        months_with_data.append(period[:7])  # Формат YYYY-MM
                
                if months_with_data:
                    months_str = ", ".join(months_with_data)
                    await update.message.reply_text(
                        f"ℹ️ Для {employee_name} є дані за {year} рік у місяцях:\n"
                        f"{months_str}\n\n"
                        f"Оберіть один з цих місяців."
                    )
                    return
        except Exception as e:
            logging.error(f"❌ Помилка пошуку по імені: {e}")
    
    # Якщо нічого не знайшли
    await update.message.reply_text(
        f"ℹ️ Дані по відпрацьованих днях відсутні для:\n"
        f"Працівник: {employee_name}\n"
        f"Рік: {year}\n"
        f"Місяць: {context.user_data.get('selected_month')}"
    )