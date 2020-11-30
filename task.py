from argparse import ArgumentParser
from traceback import format_exc
from abc import abstractmethod
from requests import get
from bs4 import BeautifulSoup as BS
from pandas import read_csv, read_excel, to_datetime


DATA_DIR = 'data/'


class Scraper:
    def __init__(self):
        self.link_info = {}

    @property
    def soup(self):
        r = get('https://www.michigan.gov/coronavirus/0,9753,7-406-98163_98173---,00.html', timeout=20)
        return BS(r.text, 'lxml')

    def get_links(self):
        datasets_header = self.soup.find('h5', text=lambda x: str(x).strip().startswith('Public Use Datasets'))
        datasets_parent = datasets_header.find_parent()
        datasets_links = datasets_parent.find_all('a')

        for link in datasets_links:
            if link.__getattribute__('text') and link.get('href'):
                self.link_info[link.text] = 'https://www.michigan.gov' + link['href']


class ExcelReader:
    def _read_excel_file(self, link_text):
        self.df = read_csv(DATA_DIR + link_text + '.csv')

    @property
    def link_info(self):
        scraper = Scraper()
        scraper.get_links()
        return scraper.link_info.copy()

    def download_remote_excel_files(self):
        self._download_remote_excel_file('Cases and Deaths by County by Date')
        self._download_remote_excel_file('Diagnostic Tests by Result and County')

    def _download_remote_excel_file(self, link_text):
        self.df = read_excel(self._get_remote_excel_file_url(link_text))

        cols = {}
        for col in self.df.columns:
            cols[col] = col.lower().replace('.', '_')

        cols['MessageDate'] = 'date'
        self.df = self.df.rename(columns=cols)

        if 'updated' in self.df.columns:
            self.df = self.df.drop(columns=['updated'])

        self.df.date = self.df.date.apply(lambda x: to_datetime(x).date())

        self.df.to_csv(DATA_DIR + link_text + '.csv', index=False)

    def _get_remote_excel_file_url(self, link_text):
        url = self.link_info.get('link_text')
        if not url:
            for text, link in self.link_info.items():
                if text.startswith(link_text):
                    return link
        return url

    def _add_rolling_average(self, roll_col):
        self.df[f'{roll_col}_rolling'] = self.df[roll_col].rolling(7).mean()


class RunnerBase(ExcelReader):
    def run(self, counties_label):
        counties_label = counties_label.lower()
        mapper = {
            'tricounty': ('Oakland', 'Macomb', 'Wayne'),
            'metro': ('Oakland', 'Macomb', 'Wayne', 'St. Clair', 'Livingston', 'Lapeer'),
            'nearby': ('Oakland', 'Macomb', 'Wayne', 'St. Clair', 'Livingston', 'Lapeer', 'Washtenaw'),
        }

        counties = mapper.get(counties_label, (counties_label.title(),))
        self._filter(counties=counties)

        if len(counties) > 1:
            self._group()
            self.df['county'] = counties_label

        self._rollup()

    @abstractmethod
    def _filter(self, *args, **kwargs):
        pass

    @abstractmethod
    def _group(self, *args, **kwargs):
        pass

    @abstractmethod
    def _rollup(self, *args, **kwargs):
        pass


class CasesRunner(RunnerBase):
    def _filter(self, **kwargs):
        counties = kwargs.get('counties')
        self._read_excel_file('Cases and Deaths by County by Date')
        self.df = self.df[self.df.county.isin(counties)]
        self.df = self.df[self.df.case_status == 'Confirmed']

    def _group(self):
        self.df = self.df.groupby(by=['date', 'case_status'], as_index=False)[['cases', 'deaths']].sum()

    def _rollup(self, **kwargs):
        self._add_rolling_average('cases')
        self._add_rolling_average('deaths')


class DiagnosticTestsRunner(RunnerBase):
    def _filter(self, **kwargs):
        counties = kwargs.get('counties')
        self._read_excel_file('Diagnostic Tests by Result and County')
        self.df = self.df[self.df.county.isin(counties)]
        self.df['positive_rate'] = self.df.positive / self.df.total

    def _group(self):
        self.df = self.df.groupby(by=['date'], as_index=False)[['negative', 'positive', 'total']].sum()
        self.df['positive_rate'] = self.df.positive / self.df.total

    def _rollup(self):
        self._add_rolling_average('positive_rate')
        self._add_rolling_average('total')


def _scrape():
    parser = ArgumentParser()
    parser.add_argument('-r', type=bool, help='refresh')
    parser.add_argument('-c', type=str, help='counties_label')
    args = parser.parse_args()
    refresh = args.r
    counties_label = args.c

    cases_runner = CasesRunner()

    if refresh:
        cases_runner.download_remote_excel_files()

    if counties_label:
        tests_runner = DiagnosticTestsRunner()

        cases_runner.run(counties_label)
        tests_runner.run(counties_label)

        combined = cases_runner.df.merge(tests_runner.df, on='date', suffixes=('_cases', '_tests'))[[
            'date', 'cases_rolling', 'deaths_rolling', 'total_rolling', 'positive_rate_rolling',
        ]].rename(columns={'total_rolling': 'tests_rolling'})

        cases_runner.df.to_csv(DATA_DIR + f'cases_{counties_label}.csv', index=False)
        tests_runner.df.to_csv(DATA_DIR + f'tests_{counties_label}.csv', index=False)
        combined.to_csv(DATA_DIR + f'combined_{counties_label}.csv', index=False)

def main():
    try:
        _scrape()
    except Exception as exc:
        open('exception.txt', 'w').write('\n\n'.join((str(exc), format_exc())))


if __name__ == '__main__':
    main()
