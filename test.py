import main
import time


def show(name):
    print(f'hi {name} ...')


def greeting():
    print(f'how are you?')


# main.every().second.do(show, 'milad') #job 1
# main.every(100).weeks.do(greeting) # job 2
main.every().hour.at('0:07').do(show, 'milad')
# main.every().hour.at(':45').do(show, 'milad')
while True:
    main.run_pending()
    # time.sleep(1)
    # main.run_all(1)
    # print(main.next_run())
    # time.sleep(60)
    time.sleep(1)
