# %%
from bs4 import BeautifulSoup

def compact_profile(html_data):
    soup = BeautifulSoup(html_data, 'html.parser')
    try:
        for item in soup.find_all("div", class_="section-header"):
            item.decompose()
        soup.find_all("div", class_="section-items")[0].decompose()
        soup.find("footer").decompose()
        for item in soup.find_all("select", class_="form-select"):
            item.decompose()
        for item in soup.find_all("div", class_="col-sm-12 text-end"):
            item.decompose()
        for item in soup.find_all("div", class_="collapse"):
            item.decompose()
        for item in soup.find_all(string="Infinite"):
            item.parent.parent.decompose()
        for item in soup.find_all(string="Infinite (%)"):
            item.parent.parent.decompose()
        for item in soup.find_all(string="Memory size"):
            item.parent.parent.decompose()

        for item in soup.find_all(string="Distinct (%)"):
            item.parent.parent.previous_sibling.find("td").insert(1, "  ~  " + item.parent.parent.find("td").text)
        for item in soup.find_all(string="Distinct (%)"):
            item.parent.parent.decompose()
        for item in soup.find_all(string="Missing (%)"):
            item.parent.parent.previous_sibling.find("td").insert(1, "  ~  " + item.parent.parent.find("td").text)
        for item in soup.find_all(string="Missing (%)"):
            item.parent.parent.decompose()

        section_items=soup.find_all("div", class_="section-items")[0]
        variables = section_items.find_all("div", class_="row sub-item")
        for variable in variables:
            try:
                variable.find_all("div", class_="col-sm-4")[1].decompose() # Remove extra table
                # variable.find_all("div", class_="col-sm-4")[1].decompose() # Remove plots
                variable.find_all("div", class_="col-sm-4")[1]["class"]="overlay"

            except:
                pass
            try:
                variable.find_all("div", class_="col-sm-6")[1].decompose() # Remove words soup
                # variable.find_all("div", class_="col-sm-6")[1]["class"]="img-overlay"
            except:
                pass

        soup.find("div", class_="section-items")['class']='card-group card-group-scroll'
        for item in soup.find_all("div", class_="row item"):
            item["class"]="card"
        for item in soup.find_all("div", class_="row sub-item"):
            item["class"]="card-body"
        for item in soup.find_all("div", class_="col-sm-4"):
            item["class"]="col-sm-12"
        for item in soup.find_all("div", class_="col-sm-6"):
            item["class"]="col-sm-12"

        head = soup.find("head")
        new_style_tag = soup.new_tag("style")
        head.append(new_style_tag)
        new_style_tag.string = """
        :root {
            font-size: 11px;
            line-height: 0.5;
        }
        .overlay {
        font-size:3.5vw;
        float:right;
        width:40%; /*important*/
        bottom:24vw; /*important*/
        padding: 5px;
        opacity: 0.7;
        z-index: -1;
        /* background:#f7f7f7; */
        position:relative;
        }
        .img-overlay {
        font-size:3.5vw;
        float:right;
        width:40%; /*important*/
        bottom:30vw; /*important*/
        padding: 5px;
        opacity: 0.6;
        z-index: -1;
        /* background:#f7f7f7; */
        position:relative;
        }
        .card {
        background-color:transparent;
        height: 210px;
        }
        .variable {
        height: 200px;
        }       
        .col-sm-12 {
        opacity: 0.9;
        }
        body {
            padding-top: 1rem;
        }
        @media (min-width: 576px) {
            .card-group.card-group-scroll {
                overflow-x: auto;
                flex-wrap: nowrap;
            }
        }
        .card-group.card-group-scroll > .card {
            flex-basis: 25%;
        }
        """
    except:
        pass
    return str(soup)
# %%
# project_id = "zgz"
# dataset_id = "zgz_mydataset"
# profile_path = f"./data/{project_id}/{dataset_id}/profile/{dataset_id}_profile.html"

# with open(profile_path) as f:
#     html_data = f.read()

# soup = compact_profile(html_data)
# soup
# %%
