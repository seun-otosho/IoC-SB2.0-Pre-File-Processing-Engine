from random import choice

from pyfiglet import FigletFont

# FigletFont.getFonts()

# fonts = [
#     'whimsy', 'univers', 'train', 'tanja', 'sweet', 'starwars', 'standard', 'speed', 'soft', 'shadow', 'script',
#     'sblood', 'rozzo', 'rounded', 'roman', 'red_phoenix', 'puffy', 'isometric1', 'isometric2', 'isometric3',
#     'isometric4', 'graffiti', 'Georgia1', 'georgi16', 'fraktur', 'flowerpower', 'dosrebel', 'doh', 'crazy', 'colossal',
#     'calgphy2', 'broadway', 'bolger', 'blocks', 'big', 'basic', 'banner3-D', 'alpha'
# ]
def font2u():
    return choice(FigletFont.getFonts())
