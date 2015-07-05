
# coding: utf-8

# In[1]:

from piazza_api import Piazza
import bs4


# In[2]:

p = Piazza()


# In[5]:

p.user_login()


# In[6]:

spark = p.network("i9esrcg0gpf8k")


# In[46]:

out =""
failed= []
for i in range(1,spark.get_statistics()["total"]["questions"]):
    try :
        post = spark.get_post(i)
        text = bs4.BeautifulSoup(post["history"][0]["content"]).getText()
        out = out +" "+  text
    except Exception, e:
        failed.append(i)


# In[48]:

len(failed)


# In[51]:

with open("out.txt", "w") as txtfile:
    txtfile.write(out.encode("utf-8"))

