import os
from PIL import Image
import cv2
import numpy as np
from numpy import asarray

Region = ['SA1', 'SA2', 'SA3', 'SA4']

for region in Region:
    for file in os.listdir('./'+region):
        print(f'Opening: {file}')
        im = Image.open('./' + region + '/' + file)
        # img = cv2.imread('./SA1/'+file)
        # print(f'Read: {buff}')
        ## change for image read
        ## convert image to np.array() called img_arr
        # kernel = np.array([[0, -1, 0],
        #                    [-1, 5, -1],
        #                    [0, -1, 0]])
        # img = cv2.filter2D(src=img, ddepth=-1, kernel=kernel)
        img_arr = asarray(im)

        with open('./' + region + '/' + region + '.csv', 'a') as f:
            for row in range(len(img_arr)):
                for col in range(len(img_arr[row])):
                    f.write(file[:-4]+','+str(row)+','+str(col)+','+str(img_arr[row][col])+'\n')
        f.close()
        print('Saving new image')
        im = Image.fromarray(img_arr)
        im.save('./tmp_watch/' + file[:-4] + ".tiff")