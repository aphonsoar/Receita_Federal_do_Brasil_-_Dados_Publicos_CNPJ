import sys

foreground = {
    'red'   : '31',
    'green' : '32',
    'yellow': '33',
    'blue'  : '34',
    'purple': '35',
    'cyan'  : '36',
    'white' : '37',
    'gray'  : '90',
}

def reset_color():
    sys.stdout.write('\033[0;0m')

def print_in_color(color, content, bold=0, break_line='\n'):
    sys.stdout.write(f'\033[{1 if bold else bold};{color}m')
    print(content, end='' if break_line == False else break_line)
    reset_color()
    sys.stdout.flush()

def print_in_red(content, bold=0, break_line='\n'):
    print_in_color(foreground['red'], content, bold, break_line=break_line)

def print_in_green(content, bold=0, break_line='\n'):
    print_in_color(foreground['green'], content, bold, break_line=break_line)

def print_in_yellow(content, bold=0, break_line='\n'):
    print_in_color(foreground['yellow'], content, bold, break_line=break_line)

def print_in_blue(content, bold=0, break_line='\n'):
    print_in_color(foreground['blue'], content, bold, break_line=break_line)

def print_in_purple(content, bold=0, break_line='\n'):
    print_in_color(foreground['purple'], content, bold, break_line=break_line)

def print_in_cyan(content, bold=0, break_line='\n'):
    print_in_color(foreground['cyan'], content, bold, break_line=break_line)

def print_in_white(content, bold=0, break_line='\n'):
    print_in_color(foreground['white'], content, bold, break_line=break_line)

def print_in_gray(content, bold=0, break_line='\n'):
    print_in_color(foreground['gray'], content, bold, break_line=break_line)