
# -*- coding: utf-8 -*-

import glob
import gzip
import tarfile
import json
import urllib.parse
import tqdm
import tqdm.contrib.concurrent
import os, sys
import re
import bs4
import urllib
from multiprocessing import Process, Manager
import multiprocessing
from IPython.core.ultratb import ColorTB
from pathlib import Path

sys.excepthook = ColorTB()

RE_spacelike = re.compile(r'\s+')
RE_brackets = re.compile(r'\(.*?\)')

# coord -> {title : (lat, lon)}
coord = {}

RE_spacelike = re.compile(r'\s+')
RE_brackets = re.compile(r'\(.*?\)')

################################################################################

def pick_coordinates(
        cirrussearch_dump, 
        output_dir,
    ):
    """This function updates:
    - coord -> {title : (lat, lon)}
    - txt file in the folder {{output_dir}/coord.tsv} -> "title lat lon id 0"

    Args:
        cirrussearch_dump (_type_): _description_
        output_dir (_type_): _description_
    """

    global coord

    with gzip.open(cirrussearch_dump, 'rt') as f, open(f'{output_dir}/coord.tsv', 'w') as wf:

        #for i, line in enumerate(f):
        i_line = 0
        while line := f.readline():
            i_line += 1
            json_data = json.loads(line)

            if 'index' in json_data:
                id = json_data['index']['_id']
            else:
                try:
                    c = json_data['coordinates']
                except:
                    print(f'Skip line {i_line}: No "coordinates" field')
                    continue
                
                if c != []:
                    t = json_data['title']
                    try:
                        c = c[0]['coord']
                    except:
                        print(f'Skip line {i_line}: No "Coord" field in "coordinates"')
                        continue

                    # text -> "title lat lon id 0"
                    text = f'{t}\t{float(c["lat"]):.5f}\t{float(c["lon"]):.5f}\t{id}\t{0}\n'
                    wf.write(text)
                    # coord -> {title : (lat, lon)}
                    coord[t] = (float(c["lat"]), float(c["lon"]), id, 0)

                    try:
                        # If the title is redirected from another source
                        redirect = json_data['redirect']
                    except:
                        print(f'Warning {i_line}: No "redirect" field')
                        continue
                                        
                    for r in redirect:
                        text = f'{r["title"]}\t{float(c["lat"]):.5f}\t{float(c["lon"]):.5f}\t{id}\t{1}\n'
                        wf.write(text)
                        coord[r['title']] = (float(c["lat"]), float(c["lon"]), id, 1)

################################################################################

def load_coord_dict(
        coord_file, 
        key=0,
    ):
    """Read an existing file {{output_dir}/coord.tsv} and saves into the local memory

    Args:
        coord_file (_type_): _description_
        key (int, optional): _description_. Defaults to 0.
    """

    global coord

    with open(coord_file, 'r') as f:
        coord_list = [l.strip().split('\t') for l in f]
        coord = {l[key]: (float(l[1]), float(l[2]), int(l[3]), int(l[4])) for l in coord_list}

################################################################################

def WHLL(
        html_dump, 
        output_dir, 
        max_worker=8, 
        max_file_read=-1,
    ):

    nb_files = 0
    job_list = []

    for i in range(392):
        root = "/Users/alexis/Documents/05b_Corpus_wiki/enwiki-NS0-20250120-ENTERPRISE-HTML.json.tar.gz"
        path_input = f"{root}/enwiki_namespace_0_{i}.ndjson"
        path_output = f"{output_dir}/enwiki_namespace_0_{i}.ndjson"
        job_list.append((path_input, path_output))
    is_tar_path = True

    # # 391
    # if html_dump.endswith('.tar.gz'):
    #     with tarfile.open(html_dump, 'r') as tarf:
    #         for filename in tarf.getnames():
    #             if filename.endswith('.ndjson'):
    #                 # Input file (compressed)
    #                 path_input = os.path.join(
    #                     html_dump, 
    #                     filename,
    #                 )
    #                 # Output
    #                 path_out = os.path.join(
    #                     output_dir, 
    #                     f"{Path(filename).stem}.ndjson",
    #                 )

    #                 job_list.append((path_input, path_out))
    #                 nb_files += 1

    #     is_tar_path = True

    #         # for member in tarf.getmembers():
    #         #     if member.name.endswith('.ndjson'):
    #         #         if nb_files == max_file_read:
    #         #             break
    #         #         # job_list.append((tarf.extractfile(member), f'{output_dir}/{os.path.basename(member.name)}.jsonl'))
    #         #         # job_list.append((tarf.extractfile(member), f'{output_dir}/{os.path.basename(member.name)}.ndjson'))
    #         #         job_list.append((member, f'{output_dir}/{os.path.basename(member.name)}.ndjson'))
    #         #         print(member, type(member))
    #         #         nb_files += 1
    #         #         type_input = "str"
    # else:
    #     job_list = [(fname, f'{output_dir}/{os.path.basename(fname)}') for fname in glob.glob(f'{html_dump}/*.ndjson')]
    #     is_tar_path = False

    job_list = job_list[:max_file_read]
    print('\u001b[44m{}\u001b[0m'.format(job_list))

    # manager = Manager()
    # coord_sync = manager.dict()
    # for key, val in coord.items():
    #     coord_sync[key] = val

    coord_list = [coord] * len(job_list)
    is_tar_path = [is_tar_path] * len(job_list)
    
    # # tqdm.contrib.concurrent.process_map(
    #     WHLL_file,
    #     job_list
    #     max_workers=max_worker,
    #     chunksize=1,
    #     ncols=0,
    # )
    
    inputs = zip(job_list, coord_list, is_tar_path)
    with multiprocessing.Pool(max_worker) as pool:
        # results = pool.starmap(WHLL_file, tqdm.tqdm(inputs, total=len(job_list)))
        res = pool.starmap(
            WHLL_file, 
            iterable=tqdm.tqdm(inputs, total=len(job_list)),
            chunksize=1,
        )

################################################################################

def WHLL_file(
        input_output, 
        dict_coords, 
        is_tar_path:bool,
    ):

    global coord

    coord = dict_coords
    input_file_pointer = input_output[0] 
    output_filename = input_output[1]

    if is_tar_path:
        html_dump = Path(input_file_pointer).parent
        filename_compressed = Path(input_file_pointer).name
        tarf = tarfile.open(html_dump, 'r')
        input_file_pointer = tarf.extractfile(filename_compressed)

    # if isinstance(input_file_pointer, str):
    else:
        # with open(input_file_pointer, 'r') as file:
            # input_file_pointer = json.load(file)
        input_file_pointer = open(input_file_pointer, 'r')

    with open(output_filename, 'w') as output_file_pointer:
        for idx_line, line in enumerate(input_file_pointer):


            json_data = json.loads(line)
            title = json_data['name']

            # New version of the dump has "Template:{title}"
            title = title.replace("Template:", "")

            # Data in {coords} -> [key lat long ID is_redirected]
            if title in coord and coord[title][3] == 0:

                if title == "Doufelgou Prefecture":
                    print('\u001b[44m{}\u001b[0m'.format((json_data['article_body']['html'])))
                    break

                soup = bs4.BeautifulSoup(json_data['article_body']['html'], 'html.parser')

                # Extract all the paragraphs (<p> tag)
                p_list = soup.find_all(name='p')

                # Dict {"text": , "gold":}
                ret_d = WHLL_article(p_list, title)

                if len(ret_d['text'].strip()) > 0:
                    d = {
                        'id': json_data['identifier'], 
                        'title': title,
                    }
                    d.update(ret_d)
                    output_file_pointer.write(
                        json.dumps(d, ensure_ascii=False) + '\n',
                    )
    
    if is_tar_path:
        tarf.close()
    input_file_pointer.close()

################################################################################

def WHLL_article(p_list, title):

    c = 0
    text = ''
    a_list = []

    # One paragraph
    for p in p_list:

        # print('\u001b[44m{}\u001b[0m'.format(p.get_text()))

        # Empty paragraph
        if p.get_text().strip() == '':
            # empty paragraph
            continue

        if p.get('class') and 'asbox-body' in p.get('class'):
            # skip asbox-body ( https://en.wikipedia.org/wiki/Template:Asbox )
            continue

        if mw := p.get('data-mw'):
            if 'Template:' in mw:
                # skip template
                continue

        sentence, annotation = WHLL_paragraph(p, title)

        for a in annotation:
            a_list.append((c+a[0], c+a[1], *a[2:5]))

        c += len(sentence)+1

        text += sentence + ' '

    if len(text) > 0:
        text = text[:-1]

    return {'text': text, 'gold': a_list}

################################################################################

def WHLL_paragraph(p, title):

    self_coord = coord[title]

    for a in p.find_all(name='a', rel='mw:WikiLink'):
        link_title = a.get('title')
        if link_title in coord:
            c = coord[link_title]
            a.attrs['lat'] = c[0]
            a.attrs['lng'] = c[1]

    title_notation = alternatename(title)
    sentence = ''
    annotation = []
    cursor_txt = 0

    for e in p:

        flg_anno_yet = True
        text = e.get_text()
        text = RE_spacelike.sub(' ', text)

        if len(text) == 0:
            continue

        if isinstance(e, bs4.element.Tag):

            if e.get('class') and 'mw-ref' in e.get('class'):
                # skip reference
                continue

            if e.name == 'style':
                # skip <style> tag
                continue

            if e.get('data-mw') and 'Template:' in e.get('data-mw'):
                # skip template
                continue

            if span_list := e.find_all(name='span'):
                all_mw = ''.join([span.get('data-mw') for span in span_list if span.get('data-mw')])
                if 'Template:' in all_mw:
                    # skip template
                    continue

            if style_list := e.find_all(name='style'):
                all_mw = ''.join([style.get('data-mw') for style in style_list if style.get('data-mw')])
                if 'Template:' in all_mw:
                    # skip template
                    continue

            # Expressions that have a geo-tag
            if e.get('lat'):

                # Target destination = {wiki_title} in http://en.wikipedia.org/wiki/{href}
                href = e['href']
                href = href.replace("./", "")
                # start = len('/wiki/')
                # href = href[start:]
                # urllib.parse.unquote(href)
                # out = f"title={title} // notation={notation} // coords={(self_coord[0], self_coord[1])} // href={href}"
                # print('\u001b[44m{}\u001b[0m'.format(out))
                href = urllib.parse.unquote(href)

                gold_data = [
                    cursor_txt, 
                    cursor_txt + len(text), 
                    text, 
                    (e.get('lat'), e.get('lng')), 
                    href,
                ]
                # print('\u001b[44m{}\u001b[0m'.format(gold_data))
                annotation.append(gold_data)
                flg_anno_yet = False

        # Pattern matching for expressions that match the title but that have no existing link/tag in Wikipedia
        if flg_anno_yet:
            for notation in title_notation:
                try:
                    #spans = [(m.start(), m.end()) for m in re.finditer(notation, text)]
                    spans = find_string_list(notation, text)
                except:
                    print(notation, text)
                    exit()
                if len(spans) > 0:
                    for span in spans:
                        href = title.replace(" ", "_")
                        # print(f"title={title} // notation={notation} // coords={(self_coord[0], self_coord[1])} // href={href}")
                        gold_data = [
                            cursor_txt + span[0], 
                            cursor_txt + span[1], 
                            notation, 
                            (self_coord[0], self_coord[1]),
                            href,
                        ]
                        annotation.append(gold_data)
                    break
                
        sentence += text
        cursor_txt += len(text)

        # sentence += p.get_text().strip()
        # cursor_txt += len(sentence)

    return sentence, annotation

################################################################################

def find_string_list(target, text):

    out = []
    cursor_txt = 0
    while cursor_txt < len(text):
        i = text[cursor_txt:].find(target)
        if i == -1:
            break
        out.append((cursor_txt+i, cursor_txt+i+len(target)))
        cursor_txt += i+len(target)
    return out

################################################################################

def alternatename(title):

    out = [title]

    if '(' in title:
        # remove brackets and content
        out.append(RE_brackets.sub('', title).strip())

    if ',' in title:
        # string before comma
        out.append(title.rsplit(',', maxsplit=1)[0].rstrip())

    return out

################################################################################

def get_id_from_wiki_title(
        wiki_title,
        dict_coord,
    ):

    wiki_title_search_in_coord = wiki_title.replace("_", " ")

    print('\u001b[44m{}\u001b[0m'.format(wiki_title_search_in_coord))
    for key in dict_coord.keys():
        if "International Air Transport" in key:
            print(key)
    if wiki_title_search_in_coord in dict_coord:
        idx = dict_coord[wiki_title_search_in_coord][2]
    else:
        idx = None

    res = {
        "wiki_id" : idx,
        "wiki_title" : wiki_title 
    }

    return res

################################################################################

if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(description='WHLL: A Python package for Wikipedia Hyperlink-based Location Linking')

    parser.add_argument(
        'cirrussearch_dump', 
        type=str,
        help='Path to the CirrusSearch dump file',
    )

    parser.add_argument(
        'html_dump', 
        type=str,
        help='Path to the HTML dump files',
    )
    parser.add_argument(
        'output_dir', 
        type=str,
        help='Path to the output directory',
    )
    parser.add_argument(
        '--make_coord', 
        action='store_true',
        help='Make coord.tsv file',
    )

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # if args.make_coord:
    #     print('Pick coordinates')
    #     pick_coordinates(args.cirrussearch_dump, args.output_dir)
    #     print('Done')
        
    load_coord_dict(f'{args.output_dir}/coord.tsv')
    
    print('WHLL')
    WHLL(args.html_dump, args.output_dir, max_file_read=1)
    print('Done')

    # REAL
    # python3 lib/WHLL/WHLL.py /Users/alexis/Documents/05b_Corpus_wiki/enwiki-20250210-cirrussearch-content.json.gz /Users/alexis/Documents/05b_Corpus_wiki/enwiki-NS0-20250120-ENTERPRISE-HTML.json.tar.gz /Users/alexis/Documents/05b_Corpus_wiki/WHLL-en-CS20250120.HTML20250120 --make_coord
    
    # TEST tar
    # python3 lib/WHLL/WHLL.py /Users/alexis/Documents/05b_Corpus_wiki/enwiki-20250210-cirrussearch-content.json.gz /Users/alexis/Documents/05b_Corpus_wiki/enwiki-NS10-20250201-ENTERPRISE-HTML.json.tar.gz /Users/alexis/Documents/05b_Corpus_wiki/test --make_coord

    # TEST uncompressed
    # python3 lib/WHLL/WHLL.py /Users/alexis/Documents/05b_Corpus_wiki/enwiki-20250210-cirrussearch-content.json.gz /Users/alexis/Documents/05b_Corpus_wiki/enwiki-NS10-20250201-ENTERPRISE-HTML.json /Users/alexis/Documents/05b_Corpus_wiki/test --make_coord

    # res = get_id_from_wiki_title(
    #     # wiki_title="International_Air_Transport_Association",
    #     wiki_title="Poznań",
    #     dict_coord=coord,
    # )
    # print('\u001b[44m{}\u001b[0m'.format(res))
    
