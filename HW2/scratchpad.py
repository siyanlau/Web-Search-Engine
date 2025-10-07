# # from ftfy import fix_text

# # with open("data/marco_tiny.tsv", "r", encoding="utf8") as f:
# #     for i, line in enumerate(f):
# #         text = fix_text(line)
# #         print(text)
# #         if i >= 10:
# #             break

# print('1946'.islower())

import os
os.makedirs("data", exist_ok=True)
content = (
"0\tApples are rich in fiber and vitamin C, and they are often eaten raw or used in pies and juice.\n"
"1\tPython is a popular programming language that emphasizes readability and rapid development.\n"
"2\tThe capital of France is Paris, which is known for its art, fashion, and gastronomy.\n"
"3\tThe process of photosynthesis converts light energy into chemical energy in plants.\n"
"4\tThe Great Wall of China was built to protect Chinese states from northern invasions.\n"
"5\tCoffee contains caffeine, a natural stimulant that can improve alertness and concentration.\n"
"6\tThe Pacific Ocean is the largest and deepest of Earth's oceanic divisions.\n"
"7\tMachine learning allows computers to learn from data without being explicitly programmed.\n"
"8\tBananas grow in tropical regions and are an important source of potassium.\n"
"9\tThe human brain consists of billions of neurons that transmit information through electrical signals.\n"
)
with open("data/toy.txt", "w", encoding="utf-8") as f:
    f.write(content)
print("✅ data/toy.txt rewritten with real TABs")
