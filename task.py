from time import sleep

import pandas as pd
import seaborn as sns
from bs4 import BeautifulSoup
from requests import Session

import mailer


class Scraper:
    _cases_filename = 'Cases and Deaths by County by Date of Onset of Symptoms and Date of Death.xlsx'
    _tests_filename = 'Diagnostic Tests by Result and County.xlsx'

    def __init__(self):
        self._page_text = None
        self._session = None

    def make_requests_and_download(self):
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


class Processor(Scraper):
    def process_and_save_remote_files(self):
        cases = self._read_local_excel_files(self._cases_filename)
        tests = self._read_local_excel_files(self._tests_filename)

        cases.to_csv('data/cases.csv', index=False)
        tests.to_csv('data/tests.csv', index=False)

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


class Roller(Processor):
    def save_rolling(self):
        self.cases_rolling.to_csv('data/cases_roll.csv', index=False)
        self.tests_rolling.to_csv('data/tests_roll.csv', index=False)

    @property
    def cases_rolling(self):
        return self._create_df_with_rolling(self._cases, ['cases_cumulative', 'deaths_cumulative'], ['cases', 'deaths'])

    @property
    def tests_rolling(self):
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


class Runner(Roller):
    def __init__(self):
        super().__init__()
        self.date = None
        self.message = None

    def refresh_and_save(self):
        self.make_requests_and_download()
        self.process_and_save_remote_files()
        self.save_rolling()

    def create_plot(self, county):
        df = self.tests_rolling.copy()
        df = df[df.county == county].copy()
        df.index = df.date
        df = df[['positive_rate', 'positive_rate_roll']]
        plot = sns.lineplot(data=df, markers=True, palette='deep')
        fig = plot.get_figure()
        fig.autofmt_xdate()
        fig.set_size_inches(12, 8)
        fig.suptitle(f'Positive rate - {county} County')
        fig.savefig(f'img/{county.lower()}.png')

    def create_message(self):
        tests_roll = self.tests_rolling.copy()
        df = tests_roll.loc[tests_roll.county.isin({'Oakland', 'Wayne', 'Macomb'}), [
            'county', 'date', 'positive_rate', 'positive_rate_roll']].drop_duplicates(subset=['county'], keep='last')
        for col in ('positive_rate', 'positive_rate_roll'):
            df[col] = df[col].apply(lambda x: round(x * 100, 1))
        records = df.to_dict('records')
        self.date = records[0]['date']
        self.message = '\n'.join('{county}: {positive_rate_roll}% (7d)'.format(**i) for i in records)


def main():
    runner = Runner()
    runner.refresh_and_save()
    runner.create_plot(county='Oakland')
    runner.create_message()
    mailer.send_email(subject=runner.date, body=runner.message)


if __name__ == '__main__':
    main()
