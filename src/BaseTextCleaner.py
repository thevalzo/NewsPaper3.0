import re
import datetime

class BaseTextCleaner:
   # def __init__(self):
        #self.db = db
        #self.feed_urls = db["feed_urls"]
        #self.articles = db["articles"]
        #self.feed_post = db["feed_post"]
       # self.cleaning_info = db["cleaning_info"]

    def clean_summary(in_raw_text):
        #remove links
        raw_text = re.sub(r'http[s]?:\/[\/]?(?:[a-zA-Z]|[0-9]|[$-_@&+]|[!*\(\),]|)+', "", in_raw_text);
        raw_text = re.sub(r"www(?:\.|[a-zA-Z]|[0-9]|[$-_@&+]|[!*\(\),]|)+", "", raw_text);
        #remove symbols
        raw_text = raw_text.replace("*", "").replace("#", "").replace("_", "")
        #remove parenteshis
        raw_text = raw_text.replace("!(", "").replace("![", "").replace("[", "").replace("]", "").replace("(", "").replace(")", "")
        return raw_text

    def clean_1(self):
        count = 0
        for article in self.articles.find(
                {"status": "raw"}
        ):
            raw_text = article["raw_text"]
            raw_text = re.sub(r'\{\{(\w|\s|\.|\-)*\}\}', "", raw_text);
            # tolgo il testo tra quadre
            raw_text = re.sub(r'\[(\w|\s|\.|\-|©|à|è|é|ò|ì|\/|,|.])*\]', "", raw_text);

            # raw_text = re.sub(r'\*(\w|\s|\.|\-|\'|\,)*\n', "", raw_text);

            raw_text = raw_text.replace("\n", " ")
            raw_text = re.sub(r"\/(?:[a-zA-Z]|[0-9]|[$-_@&+]|[!*,])*", "", raw_text);
            raw_text = re.sub(r"([a-zA-Z]|[0-9]|\-)+\.([a-zA-Z]|[0-9]|\-)+(\?([a-zA-Z]|[0-9]|\=)+)?", "", raw_text);
            raw_text = re.sub(r"\\'", " ", raw_text);
            raw_text = raw_text.replace("*", "").replace("#", "").replace("_", "").replace("!(", "").replace("![", "")
            raw_text = re.sub(r"[\s]+"," ", raw_text);
            clean_text = raw_text.strip()

            self.articles.update({"_id": article.get('_id')}, {"$set": {"status": "clean1", "clean1_text": clean_text}})
            count += 1
        print("Updated "+str(count) + " articles to status clean1")

    def long_substr(self, data):
        substr = ''
        if len(data) > 1 and len(data[0]) > 0:
            for i in range(len(data[0])):
                for j in range(len(data[0])-i+1):
                    if j > len(substr) and all(data[0][i:i+j] in x for x in data):
                        substr = data[0][i:i+j]
        return substr


    def clean_2_update_clean_info(self):
        for article in self.articles.find(
                #{"status": "clean1"}
        ):

            self.articles.update({"_id": article.get('_id')},
                                 {"$set": {"clean2_text": article['clean1_text']}})
        for feed_name in self.feed_urls.find().distinct('feed_name'):
            for category in self.articles.find({"source":feed_name}).distinct('category'):
                removelist = []
                sample_list = []
                i = 0
                print("Calculating long substr for feed "+feed_name+" and category " + category + "..")
                for i in range(0, 2):
                    sample = list(self.db.articles.aggregate([{"$match": {"source": feed_name, "category":  category}}, {"$sample": {"size": 3}}]))



                    if sample.__len__()>2:
                        sample_link = [sample[0]["link"], sample[1]["link"], sample[2]["link"]]
                        sample_list.append(sample_link)
                        print(sample[0]["link"])

                        temp_1 = sample[0]["clean2_text"].split('.')
                        start_1 = temp_1[0] +'.'+ temp_1[1] +'.'+ temp_1[2]+'.'+ temp_1[3]
                        print(start_1+ '\n')
                        temp_2 = sample[1]["clean2_text"].split('.')
                        start_2 = temp_2[0] +'.'+ temp_2[1] +'.'+ temp_2[2] +'.'+ temp_2[3]
                        temp_3 = sample[2]["clean2_text"].split('.')
                        start_3 = temp_3[0] +'.'+ temp_3[1] +'.'+ temp_3[2]+'.'+ temp_3[3]
                        to_remove_start = self.long_substr([start_1, start_2, start_3])

                        temp_1 = sample[0]["clean2_text"].split('.')
                        end_1 = temp_1[-4] +'.'+ temp_1[-3] +'.'+ temp_1[-2] +'.'+ temp_1[-1]
                        print(end_1+ '\n')
                        temp_2 = sample[1]["clean2_text"].split('.')
                        end_2 = temp_2[-4] +'.'+temp_2[-3] +'.'+ temp_2[-2] +'.'+ temp_2[-1]
                        temp_3 = sample[2]["clean2_text"].split('.')
                        end_3 = temp_3[-4] +'.'+ temp_3[-3] +'.'+ temp_3[-2] +'.'+ temp_3[-1]
                        to_remove_end = self.long_substr(
                        [end_1, end_2, end_3])

                    else:
                        to_remove_start = ""
                        to_remove_end = ""

                    if (len(to_remove_start ) > 15):
                        removelist.append(to_remove_start.strip())
                    if (len(to_remove_end ) > 15):
                        removelist.append(to_remove_end.strip())
                    count = 0
                    if removelist.__len__() != 0:
                        for article in self.articles.find({"source": feed_name, "category":  category}):
                            clean1_text = article["clean2_text"]
                            clean2_text = clean1_text
                            clean2_text = clean2_text.replace(to_remove_start, "")
                            clean2_text = clean2_text.replace(to_remove_end, "")
                            self.articles.update({"_id": article.get('_id')},
                                                 {"$set": {"clean2_text": clean2_text}})
                            count += 1
                    i+=1
                print("Phase 2 : cleaned " + str(count) + " articles for feed " + str(feed_name) + " and category "+category)
                print(removelist)
                obj = {"feed" : feed_name,
                       "category": category,
                       "remove_list" : removelist,
                       "sample_link": sample_list,
                       "date" : datetime.datetime.now()
                }
                self.cleaning_info.insert_one(obj)

    def clean_2(self):
        for feed in self.feed_urls.find():
            removelist_obj = self.cleaning_info.find({"feed": feed['feed_name']}).sort({"date": -1}).limit(1)

            #print("Cleaning articles phase 2 for feed "+str(feed['feed_name']))
            count = 0
            for article in self.articles.find({"status": "clean1", "source": feed['feed_name']}):
                clean1_text = article["clean1_text"]
                clean2_text = clean1_text
                for el in removelist_obj['remove_list']:
                    clean2_text = clean2_text.replace(el, "")

                self.articles.update({"_id": article.get('_id')},
                                {"$set": {"status": "clean2", "clean2_text": clean2_text}})
                count += 1
            print("Phase 2 : cleaned "+str(count)+" articles for feed "+str(feed['feed_name']))