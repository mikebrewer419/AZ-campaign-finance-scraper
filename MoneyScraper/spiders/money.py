from gc import callbacks
import scrapy
import json
from peewee import *
from datetime import date

cur_text = "curl 'https://seethemoney.az.gov/Reporting/AdvancedSearch/?CommiteeReportId=&CategoryType={CategoryType}&JurisdictionId=0&CycleId={CycleId}&StartDate={StartDate}&EndDate={EndDate}&FilerName=&FilerId=&BallotName=&BallotMeasureId=&FilerTypeId={FilterTypeId}&OfficeTypeId=&OfficeId=&PartyId=&ContributorName=&VendorName=&StateId=&City=&Employer=&Occupation=&CandidateName=&CandidateFilerId=&Position=Support&LowAmount=&HighAmount=' \
  -H 'authority: seethemoney.az.gov' \
  -H 'accept: application/json, text/javascript, */*; q=0.01' \
  -H 'accept-language: en-US,en;q=0.9' \
  -H 'content-type: application/x-www-form-urlencoded; charset=UTF-8' \
  -H 'cookie: _gcl_au=1.1.1149628796.1651004422' \
  -H 'origin: https://seethemoney.az.gov' \
  -H 'referer: https://seethemoney.az.gov/Reporting/AdvancedSearch/' \
  -H 'sec-ch-ua: \" Not A;Brand\";v=\"99\", \"Chromium\";v=\"101\", \"Google Chrome\";v=\"101\"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: \"Windows\"' \
  -H 'sec-fetch-dest: empty' \
  -H 'sec-fetch-mode: cors' \
  -H 'sec-fetch-site: same-origin' \
  -H 'user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36' \
  -H 'x-requested-with: XMLHttpRequest' \
  --data-raw 'draw=3&columns%5B0%5D%5Bdata%5D=TransactionDate&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=CommitteeName&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=Amount&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=TransactionName&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=TransactionType&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=Occupation&columns%5B5%5D%5Bname%5D=&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=Employer&columns%5B6%5D%5Bname%5D=&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=true&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=City&columns%5B7%5D%5Bname%5D=&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=true&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B8%5D%5Bdata%5D=State&columns%5B8%5D%5Bname%5D=&columns%5B8%5D%5Bsearchable%5D=true&columns%5B8%5D%5Borderable%5D=true&columns%5B8%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B8%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B9%5D%5Bdata%5D=ZipCode&columns%5B9%5D%5Bname%5D=&columns%5B9%5D%5Bsearchable%5D=true&columns%5B9%5D%5Borderable%5D=true&columns%5B9%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B9%5D%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=0&order%5B0%5D%5Bdir%5D=asc&start={start}&length=10&search%5Bvalue%5D=&search%5Bregex%5D=false' \
  --compressed"
db = SqliteDatabase('records.db')

class ArizonaCandidateContribution(Model):
    transaction_date = CharField(null=True)
    filer_name = CharField(null=True)
    amount = FloatField(null=True)
    tran_name = CharField(null=True)
    tran_type = CharField(null=True)
    occupation = CharField(null=True)
    employer = CharField(null=True)
    city = CharField(null=True)
    state = CharField(null=True)
    zip = CharField(null=True)
    
    class Meta:
        database = db
        table_name = 'arizona_candidate_contributions'

class ArizonaCommitteeContribution(Model):
    transaction_date = CharField(null=True)
    filer_name = CharField(null=True)
    amount = FloatField(null=True)
    tran_name = CharField(null=True)
    tran_type = CharField(null=True)
    occupation = CharField(null=True)
    employer = CharField(null=True)
    city = CharField(null=True)
    state = CharField(null=True)
    zip = CharField(null=True)
    
    class Meta:
        database = db
        table_name = 'arizona_committee_contributions'

db.connect()
db.create_tables([ArizonaCandidateContribution,ArizonaCommitteeContribution])


class MoneySpider(scrapy.Spider):
    name = 'money'
    filter_types = [130, 131, 132, 96]
    category_types = []
    
    def start_requests(self):
        request = scrapy.Request.from_curl(cur_text.format(
            CategoryType='Income',
            CycleId='39~1%2F1%2F2021%2012%3A00%3A00%20AM~12%2F31%2F2022%2011%3A59%3A59%20PM',
            StartDate='2022-01-01',
            EndDate='2022-12-31',
            FilterTypeId=130,
            start=0
        ), callback=self.parse_candidate_income)
        yield request
        request = scrapy.Request.from_curl(cur_text.format(
            CategoryType='Income',
            CycleId='39~1%2F1%2F2021%2012%3A00%3A00%20AM~12%2F31%2F2022%2011%3A59%3A59%20PM',
            StartDate='2022-01-01',
            EndDate='2022-12-31',
            FilterTypeId=131,
            start=0
        ), callback=self.parse_committee_income)
        yield request

    def parse_candidate_income(self, response):
        res = json.loads(response.text)
        total_count = res['recordsFiltered']
        for s in range(0, total_count, 10):
            request = scrapy.Request.from_curl(
                cur_text.format(
                    CategoryType='Income',
                    CycleId='39~1%2F1%2F2021%2012%3A00%3A00%20AM~12%2F31%2F2022%2011%3A59%3A59%20PM',
                    StartDate='2022-01-01',
                    EndDate='2022-12-31',
                    FilterTypeId=130,
                    start=s
                ), callback=self.save_candidate_income
            )
            yield request
    
    def save_candidate_income(self, response):
        data = json.loads(response.text)['data']
        for row in data:
            print(date.fromtimestamp(int(row['TransactionDate'][6:-5])).__str__())
            print(row)
            rec = ArizonaCandidateContribution(
                transaction_date=date.fromtimestamp(int(row['TransactionDate'][6:-5])).__str__(),
                filer_name=row['FilerName'],
                amount=row['Amount'],
                tran_name=row['TransactionName'],
                tran_type=row['TransactionType'],
                occupation=row['Occupation'],
                employer=row['Employer'],
                city=row['City'],
                state=row['State'],
                zip=row['ZipCode']
            )
            rec.save()
    
    def parse_committee_income(self, response):
        res = json.loads(response.text)
        total_count = res['recordsFiltered']
        for s in range(0, total_count, 10):
            request = scrapy.Request.from_curl(
                cur_text.format(
                    CategoryType='Income',
                    CycleId='39~1%2F1%2F2021%2012%3A00%3A00%20AM~12%2F31%2F2022%2011%3A59%3A59%20PM',
                    StartDate='2022-01-01',
                    EndDate='2022-12-31',
                    FilterTypeId=131,
                    start=s
                ), callback=self.save_committee_income
            )
            yield request
    
    def save_committee_income(self, response):
        data = json.loads(response.text)['data']
        for row in data:
            print(date.fromtimestamp(int(row['TransactionDate'][6:-5])).__str__())
            print(row)
            rec = ArizonaCommitteeContribution(
                transaction_date=date.fromtimestamp(int(row['TransactionDate'][6:-5])).__str__(),
                filer_name=row['FilerName'],
                amount=row['Amount'],
                tran_name=row['TransactionName'],
                tran_type=row['TransactionType'],
                occupation=row['Occupation'],
                employer=row['Employer'],
                city=row['City'],
                state=row['State'],
                zip=row['ZipCode']
            )
            rec.save()

