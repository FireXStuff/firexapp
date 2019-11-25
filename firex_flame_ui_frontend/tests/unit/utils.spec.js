import _ from 'lodash';
import { createLinkedHtml, createLinkifyRegex } from '@/utils';


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
});
