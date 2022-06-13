from email.mime.text import MIMEText
from os import environ
from smtplib import SMTP_SSL
from time import sleep

import pandas as pd
import seaborn as sns
from bs4 import BeautifulSoup
from requests import Session


class Scraper:
    def __init__(self):
        self._cases_filename = 'Cases and Deaths by County by Date of Onset of Symptoms and Date of Death.xlsx'
        self._tests_filename = 'Diagnostic Tests by Result and County.xlsx'
        self._page_text = None
        self._session = None

    def _make_requests_and_download(self):
        self._session = Session()
        self._make_request_to_main_page()
        self._download_remote_files()
        self._session.close()

    def _make_request_to_main_page(self):
        r = self._session.get('https://www.michigan.gov/coronavirus/0,9753,7-406-98163_98173---,00.html')
        sleep(2)
        self._page_text = r.text

    def _download_remote_files(self):
        links = self._get_links_to_remote_files()
        for fn in (self._cases_filename, self._tests_filename):
            r = self._session.get(links[fn.rsplit('.', 1)[0]])
            sleep(2)
            open('files/' + fn, 'wb').write(r.content)

    def _get_links_to_remote_files(self):
        soup = BeautifulSoup(self._page_text, 'lxml')
        datasets_links = soup.find('h5', text=lambda x: str(x).strip().startswith(
            'Public Use Datasets')).find_parent().find_all('a')

        links = {}
        for link in datasets_links:
            if link.get('href'):
                link_text = ' '.join(link.text.split())
                links[link_text] = 'https://www.michigan.gov' + link['href']

        return links


class Roller(Scraper):
    def __init__(self):
        super().__init__()
        self.date = None
        self.message = None
        self.tricounty = ('Oakland', 'Wayne', 'Macomb')

    def refresh_and_save(self):
        self._make_requests_and_download()
        self._process_and_save_remote_files()
        self._save_rolling()

    def create_plot(self, county: str = None, counties: tuple = None):
        df = self._tests_rolling.copy()
        df = df[df.county.isin((county,) if county else counties)].copy()
        df.index = df.date
        df = df[['positive_rate', 'positive_rate_roll']]
        plot = sns.lineplot(data=df, markers=True, palette='deep')
        fig = plot.get_figure()
        fig.autofmt_xdate()
        fig.set_size_inches(12, 8)
        label = county if county else '-'.join(counties)
        fig.suptitle(f'Positive rate - {label}')
        fig.savefig(f'img/{label.lower()}.png')

    def create_message(self):
        tests_roll = self._tests_rolling.copy()
        df = tests_roll.loc[tests_roll.county.isin(self.tricounty), [
            'county', 'date', 'positive_rate', 'positive_rate_roll']].drop_duplicates(subset=['county'], keep='last')
        for col in ('positive_rate', 'positive_rate_roll'):
            df[col] = df[col].apply(lambda x: round(x * 100, 1))
        records = df.to_dict('records')
        self.date = records[0]['date']
        self.message = '\n'.join('{county}: {positive_rate_roll}% (7d)'.format(**i) for i in records)

    def _process_and_save_remote_files(self):
        cases = self._read_local_excel_files(self._cases_filename)
        tests = self._read_local_excel_files(self._tests_filename)
        cases.to_csv('data/cases.csv', index=False)
        tests.to_csv('data/tests.csv', index=False)

    def _save_rolling(self):
        self._cases_rolling.to_csv('data/cases_roll.csv', index=False)
        self._tests_rolling.to_csv('data/tests_roll.csv', index=False)

    def _read_local_excel_files(self, fn):
        df = pd.read_excel('files/' + fn)
        return self._modify_dataframe(df)

    @staticmethod
    def _modify_dataframe(df):
        cols = dict((col, col.lower().replace('.', '_')) for col in df.columns)
        cols['MessageDate'] = 'date'
        df = df.rename(columns=cols)
        df.date = df.date.apply(lambda x: pd.to_datetime(x).date())
        return df

    def _create_df_with_rolling(self, df, drop_cols, roll_cols):
        df = df.drop(columns=drop_cols)
        if 'updated' in df.columns:
            df = df.drop(columns='updated')
        return pd.concat([self._add_rolling_averages(df, county, roll_cols) for county in self._counties])

    @staticmethod
    def _add_rolling_averages(df, county, roll_cols):
        df = df[df['county'] == county].copy()
        for col in roll_cols:
            df[f'{col}_roll'] = df[col].rolling(7).mean()
        return df

    @property
    def _cases_rolling(self):
        return self._create_df_with_rolling(self._cases, ['cases_cumulative', 'deaths_cumulative'], ['cases', 'deaths'])

    @property
    def _tests_rolling(self):
        tests = self._tests.copy()
        tests['positive_rate'] = tests['positive'] / tests['total']
        return self._create_df_with_rolling(tests, ['negative'], ['positive_rate'])

    @property
    def _cases(self):
        return pd.read_csv('data/cases.csv')

    @property
    def _tests(self):
        return pd.read_csv('data/tests.csv')

    @property
    def _counties(self):
        return self._cases['county'].unique()


def send_email(subject: str, body: str):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = environ['EMAIL_SENDER']
    msg['To'] = environ['RECIPIENT']

    server = SMTP_SSL(host='smtp.gmail.com', port=465)
    server.login(environ['EMAIL_SENDER'], environ['EMAIL_PASSWORD'])
    server.send_message(msg)
    server.quit()


def main():
    r = Roller()
    r.refresh_and_save()
    r.create_plot(county='Oakland')
    r.create_message()
    send_email(subject=r.date, body=r.message)


if __name__ == '__main__':
    main()
