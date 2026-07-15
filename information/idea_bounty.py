# information/idea_bounty.py
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext

IDEA_BOUNTY_TEXT = (
    "💡💰 *ПОЛЮВАННЯ ЗА ІДЕЯМИ — ваша ідея варта грошей!*\n\n"
    "Цей бот створюється для вас — тож кому, як не вам, знати, "
    "чого йому не вистачає?\n\n"
    "Оголошуємо постійну винагороду:\n\n"
    "💵 *500 грн за кожну ідею нової фічі для бота, "
    "яку буде затверджено та прийнято до виконання!*\n\n"
    "━━━━━━━━━━━━━━━\n\n"
    "📬 *Як запропонувати ідею:*\n"
    "1. Напишіть листа на *od@ftpua.com*\n"
    "2. У темі листа обов'язково вкажіть: *«ідея»*\n"
    "3. Опишіть свою ідею: що саме має вміти бот і чим це буде корисно\n\n"
    "Ми уважно розглянемо кожну пропозицію. Якщо ідею буде затверджено "
    "і вона піде в роботу — *вам буде нараховано 500 грн.* 💰\n\n"
    "⚡ *Хто перший — того й винагорода!*\n"
    "Якщо однакову ідею запропонують кілька людей — винагороду "
    "отримує той, чий лист надійшов першим. 🏁\n\n"
    "Думайте сміливо. Пропонуйте! 🚀"
)


async def show_idea_bounty(update: Update, context: CallbackContext) -> None:
    """
    Відображає правила конкурсу ідей для нових фіч бота.
    """
    context.user_data['menu'] = 'idea_bounty'

    back_button = KeyboardButton("Назад")
    main_menu_button = KeyboardButton("Головне меню")
    reply_markup = ReplyKeyboardMarkup(
        [[back_button, main_menu_button]],
        one_time_keyboard=True,
        resize_keyboard=True
    )

    await update.message.reply_text(IDEA_BOUNTY_TEXT, parse_mode="Markdown", reply_markup=reply_markup)
