import _ from 'lodash';
import {
  createLinkedHtml, createLinkifyRegex, flatGoogleBucketsListingToFilesByDir, expandDirs,
} from '@/utils';
import EXAMPLE_GOOGLE_BUCKET_ITEMS from './test_data/example-google-bucket-items.json';
import EXPECTED_PARSED_GOOGLE_BUCKET from
  './test_data/expectations/expect-parsed-google-bucket-items.json';

function makeLink(content) {
  return `<a href="${content}" class="subtle-link">${content}</a>`;
}


const REGEX = createLinkifyRegex(['/auto/', '/ws/']);

describe('utils.js', () => {
  it('linkifies http urls', () => {
    const input = 'http://some-host.com/path';
    const result = createLinkedHtml(input, REGEX);
    expect(result).toEqual(makeLink(input));
  });
  it('linkifies https urls', () => {
    const input = 'https://some-host.com/path';
    const result = createLinkedHtml(input, REGEX);
    expect(result).toEqual(makeLink(input));
  });
  it('linkifies /auto paths', () => {
    const input = '/auto/some/path';
    const result = createLinkedHtml(input, REGEX);
    expect(result).toEqual(makeLink(input));
  });
  it('linkifies /ws paths', () => {
    const input = '/ws/some/path';
    const result = createLinkedHtml(input, REGEX);
    expect(result).toEqual(makeLink(input));
  });
  it('stops considering links with &quot;', () => {
    const input = 'http://server/ws/some/path"';
    const result = createLinkedHtml(input, REGEX);
    expect(result).toEqual(makeLink('http://server/ws/some/path') + '&quot;');
  });
  it('stops considering links with &lt;', () => {
    const input = 'http://server/ws/some/path<';
    const result = createLinkedHtml(input, REGEX);
    expect(result).toEqual(makeLink('http://server/ws/some/path') + '&lt;');
  });
  it('stops considering links with &#39;', () => {
    const input = "http://server/ws/some/path'";
    const result = createLinkedHtml(input, REGEX);
    expect(result).toEqual(makeLink('http://server/ws/some/path') + '&#39;');
  });
  it('linkifies html links', () => {
    const input = '<a href="https://some.com/link">https://some.com/link</a>';
    const result = createLinkedHtml(input, REGEX);

    const escapedReplace = _.escape('<a href="REPLACE_ME">REPLACE_ME</a>');
    const expected = escapedReplace.replace(/REPLACE_ME/g,
      makeLink('https://some.com/link'));
    expect(result).toEqual(expected);
  });
  it('expand directories', () => {
    const expandedDirs = expandDirs([['a', 'b', 'c'], ['a', 'b', 'd'], ['a', 'x', 'y', 'z', 'k']]);
    const expected = [
      [],
      ['a'],
      ['a', 'b'],
      ['a', 'b', 'c'],
      ['a', 'b', 'd'],
      ['a', 'x'],
      ['a', 'x', 'y'],
      ['a', 'x', 'y', 'z'],
      ['a', 'x', 'y', 'z', 'k']];
    expect(expandedDirs).toEqual(expected);
  });
  it('extracts directory tree from google buckets file listing', () => {
    const firexId = 'FireX-djungic-201205-002928-35988';
    const result = flatGoogleBucketsListingToFilesByDir(EXAMPLE_GOOGLE_BUCKET_ITEMS, firexId);
    expect(result).toEqual(EXPECTED_PARSED_GOOGLE_BUCKET);
  });
});
