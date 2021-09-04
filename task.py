from datetime import datetime
from os import system
from time import sleep
from traceback import format_exc
import pandas as pd
from bs4 import BeautifulSoup
from requests import get


class Scraper:
    _page_filepath = 'data/page.html'
    _cases_filename = 'Cases and Deaths by County by Date of Onset of Symptoms and Date of Death.xlsx'
    _tests_filename = 'Diagnostic Tests by Result and County.xlsx'

    def make_request_to_main_page(self):
        r = get('https://www.michigan.gov/coronavirus/0,9753,7-406-98163_98173---,00.html')
        open(self._page_filepath, 'wb').write(r.content)
        sleep(2)

    def download_remote_files(self):
        links = self._get_links_to_remote_files()
        for fn in (self._cases_filename, self._tests_filename):
            r = get(links[fn.rsplit('.', 1)[0]])
            open('data/' + fn, 'wb').write(r.content)
            sleep(2)

    def _get_links_to_remote_files(self):
        soup = BeautifulSoup(open(self._page_filepath).read(), 'lxml')
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

        cases.to_csv('cases.csv', index=False)
        tests.to_csv('tests.csv', index=False)

    def _read_local_excel_files(self, fn):
        df = pd.read_excel('data/' + fn)
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
        self.cases_rolling.to_csv('cases_roll.csv', index=False)
        self.tests_rolling.to_csv('tests_roll.csv', index=False)

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
        return pd.read_csv('cases.csv')

    @property
    def _tests(self):
        return pd.read_csv('tests.csv')

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
    def run(self):
        self.refresh_and_save()
        self._generate_text_to_display()
        self._commit_and_push()

    def refresh_and_save(self):
        self.make_request_to_main_page()
        self.download_remote_files()
        self.process_and_save_remote_files()
        self.save_rolling()

    def _generate_text_to_display(self):
        tests_roll = pd.read_csv('tests_roll.csv')
        county = tests_roll[tests_roll.county == 'Oakland'].tail(1).to_dict('records')[0]
        self._text_to_display = '\n'.join(f'{key}: {round(county[key], 2)}' for key in (
            'date', 'positive_rate', 'positive_rate_roll'))

    def _commit_and_push(self):
        commands = '''
git config user.name "Automated"
git config user.email "actions@users.noreply.github.com"
git add -A
git commit -m "Latest data: {0}\n\n{1}" || exit 0
git push
'''.strip().format(datetime.utcnow().strftime('%d %B %Y %H:%M'), self._text_to_display)
        for command in commands.splitlines():
            system(command)


def main():
    try:
        Runner().run()
    except Exception as exc:
        open('exception.txt', 'w').write('\n\n'.join((str(exc), format_exc())))


if __name__ == '__main__':
    main()
