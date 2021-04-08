from argparse import ArgumentParser
from traceback import format_exc
from bs4 import BeautifulSoup as BS
from pandas import read_csv, read_excel, to_datetime, concat
from requests import get


DATA_DIR = 'data/'


class Scraper:
    @property
    def _soup(self):
        r = get('https://www.michigan.gov/coronavirus/0,9753,7-406-98163_98173---,00.html', timeout=20)
        return BS(r.text, 'lxml')

    @property
    def _link_info(self):
        link_info = {}

        datasets_header = self._soup.find('h5', text=lambda x: str(x).strip().startswith('Public Use Datasets'))
        datasets_links = datasets_header.find_parent().find_all('a')

        for link in datasets_links:
            if link.get('href'):
                link_info[link.text] = 'https://www.michigan.gov' + link['href']

        return link_info


class Downloader(Scraper):
    def __init__(self):
        self._links = None
        self._cases = None
        self._tests = None

    def read_files_from_disk(self):
        _read = lambda link_text: read_csv(DATA_DIR + link_text + '.csv')

        self._cases = _read('Cases and Deaths by County by Date')
        self._tests = _read('Diagnostic Tests by Result and County')

    def download_remote_files(self):
        self._links = self._link_info.copy()

        self._cases = self._download_remote_excel_file('Cases and Deaths by County by Date')
        self._tests = self._download_remote_excel_file('Diagnostic Tests by Result and County')

    def _download_remote_excel_file(self, link_text):
        df = read_excel(self._get_remote_excel_file_url(link_text))

        cols = {}
        for col in df.columns:
            cols[col] = col.lower().replace('.', '_')

        cols['MessageDate'] = 'date'
        df = df.rename(columns=cols)

        df['date'] = df['date'].apply(lambda x: to_datetime(x).date())
        df.to_csv(DATA_DIR + link_text + '.csv', index=False)
        return df

    def _get_remote_excel_file_url(self, link_text):
        url = self._links.get('link_text')
        if not url:
            for text, link in self._links.items():
                if text.startswith(link_text):
                    return link
        return url


class Roller(Downloader):
    @property
    def _counties(self):
        return self._cases['county'].unique()

    @property
    def cases_rolling(self):
        return self._create_df_with_rolling(self._cases, ['cases_cumulative', 'deaths_cumulative'], ['cases', 'deaths'])

    @property
    def tests_rolling(self):
        tests = self._tests.copy()
        tests['positive_rate'] = tests['positive'] / tests['total']
        return self._create_df_with_rolling(tests, ['negative'], ['positive_rate'])

    def _create_df_with_rolling(self, df, drop_cols, roll_cols):
        df = df.drop(columns=drop_cols)
        if 'updated' in df.columns:
            df = df.drop(columns='updated')
        return concat([_add_rolling_averages(df, county, roll_cols) for county in self._counties], sort=False)


def _add_rolling_averages(df, county, roll_cols):
    df = df[df['county'] == county].copy()
    for col in roll_cols:
        df[f'{col}_roll'] = df[col].rolling(7).mean()
    return df

def _run():
    parser = ArgumentParser()
    parser.add_argument('-r', type=bool, help='refresh from source?')
    parser.add_argument('-m', type=bool, help='view message only')
    args = parser.parse_args()

    roller = Roller()
    if args.r:
        roller.download_remote_files()
    else:
        roller.read_files_from_disk()

    tests_rolling = roller.tests_rolling.copy()
    if args.m:
        tr = tests_rolling[tests_rolling.county == 'Oakland'].tail(1).reset_index(drop=True).to_dict('records')[0]
        for key in ('date', 'positive_rate', 'positive_rate_roll'):
            print(f'{key}: {tr[key]}')
    else:
        roller.cases_rolling.to_csv(DATA_DIR + 'cases_roll.csv', index=False)
        tests_rolling.to_csv(DATA_DIR + 'tests_roll.csv', index=False)

def main():
    try:
        _run()
    except Exception as exc:
        open('exception.txt', 'w').write('\n\n'.join((str(exc), format_exc())))


if __name__ == '__main__':
    main()
