
import requests
import pandas as pd
from datetime import datetime as dt
import numpy as np

# start & end date of 9th European Parliament
start_term = dt.strptime("2019-07-02","%Y-%m-%d")
end_term = dt.strptime("2024-06-05","%Y-%m-%d")


def get_all_meps_with_details():
    df_cm = get_current_meps()
    df_md = get_meps_details(list(df_cm.identifier))
    df = pd.merge(left=df_cm, right=df_md, how='inner', on='identifier', suffixes=(None, "_det"))
    df.drop(columns=['placeOfBirth', 'familyName', 'givenName', 'sortLabel', 'hasMembership', 'officialFamilyName',
                     'officialGivenName', 'id_det', 'type_det', 'label_det', 'notation_codictPersonId', 'hasGender',
                     'hasHonorificPrefix', 'citizenship', 'familyName_det', 'givenName_det', 'sortLabel_det',
                     'upperFamilyName', 'upperGivenName', 'officialFamilyName_det', 'officialGivenName_det',
                     'upperOfficialFamilyName', 'upperOfficialGivenName'], inplace=True)

    return df


def get_current_meps():
    response_json = requests.get(url="https://data.europarl.europa.eu/api/v1/meps/show-current?format=application%2Fld%2Bjson&offset=0").json()
    return pd.json_normalize(response_json, "data")


def get_meps_details(ids):
    df = pd.DataFrame()

    for chunk in chunks(ids, 100):
        str_ids = str(chunk)[1:-1].replace("'", "").replace(" ", "")
        response_json = requests.get(url="https://data.europarl.europa.eu/api/v1/meps/" + str_ids + "?format=application%2Fld%2Bjson").json()
        df = pd.concat([df, pd.json_normalize(response_json, "data")])

    df.reset_index(inplace=True)

    for i in range(len(df)):
        df_mem = pd.json_normalize(df.iloc[i]['hasMembership'])
        id = df.iloc[i]['identifier']
        df_mem = df_mem[(df_mem['organization'] == 'org/ep-9') & ((df_mem['role'] == 'http://publications.europa.eu/resource/authority/role/MEMBER')|(df_mem['role'] == 'http://publications.europa.eu/resource/authority/role/MEMBER_EP'))]
        df_mem.reset_index(inplace=True)
        end_date = (df_mem['memberDuring.endDate'][0]) if 'memberDuring.endDate' in df_mem else end_term
        start_date = dt.fromisoformat(df_mem['memberDuring.startDate'][0]).date() if 'memberDuring.startDate' in df_mem else start_term

        end_date = end_term if np.nan else end_date
        df.at[i, 'start_date'] = start_date
        df.at[i, 'end_date'] = end_date

        df['start_date'] = pd.to_datetime(df['start_date']).dt.date
        df['end_date'] = pd.to_datetime(df['end_date']).dt.date

    return df


def chunks(items_list, chunk_size):
    return [items_list[i * chunk_size:(i + 1) * chunk_size] for i in
            range((len(items_list) + chunk_size - 1) // chunk_size)]


def get_all_events_with_details():
    df_events = get_all_events()
    df_details = get_events_details(list(df_events.activity_id))

    df = pd.merge(left=df_events, right=df_details, how='inner', on='id')
    df.drop(columns=['type_y', 'activity_id_y', 'had_activity_type_y', 'scheduledIn', 'eli-dl:activity_date.@value',
                     'eli-dl:activity_date.type', 'activity_label.sk', 'activity_label.hr', 'activity_label.lv',
                     'activity_label.nl', 'activity_label.pl', 'activity_label.mt', 'activity_label.en',
                     'activity_label.fi', 'activity_label.es', 'activity_label.pt', 'activity_label.hu',
                     'activity_label.fr', 'activity_label.el', 'activity_label.et', 'activity_label.da',
                     'activity_label.bg', 'activity_label.it', 'activity_label.sl', 'activity_label.de',
                     'activity_label.sv', 'activity_label.cs', 'activity_label.lt', 'activity_label.ro',
                     'documented_by_a_realization_of', 'eli-dl:recorded_in_realization_of', 'consists_of',
                     'recorded_in_a_realization_of', 'eli-dl:recorded_in_realization_of.id', 'activity_label.ga'], inplace=True)

    df.rename(columns={"type_x": "type", "activity_id_x": "activity_id", "had_activity_type_x": "had_activity_type"}, inplace=True)

    df['activity_date'] = pd.to_datetime(df['activity_date'])

    df['had_excused_person'] = "'" + df['had_excused_person'].map(str) + "'"
    df['had_participant_person'] = "'" + df['had_participant_person'].map(str) + "'"

    df["number_of_attendees"] = pd.to_numeric(df["number_of_attendees"])
    df = df[df['number_of_attendees'] > 0]
    #df.to_csv('events_with_details.csv')
    return df


def get_events_details(activities):
    df = pd.DataFrame()

    for chunk in chunks(activities, 25):
        str_ids = str(chunk)[1:-1].replace("'", "").replace(" ", "")
        response = requests.get(
            url="https://data.europarl.europa.eu/api/v1/events/" + str_ids + "?format=application%2Fld%2Bjson&json-layout=framed")
        response_json = response.json()

        df = pd.concat([df, pd.json_normalize(response_json, "data").convert_dtypes()])

    df.activity_date = pd.to_datetime(df.activity_date)
    return df[(df.type == 'Activity') & (df.activity_date >= dt(2019, 7, 2)) & (df.activity_date < dt.today())]


def get_all_events():
    response_json = requests.get(
        url="https://data.europarl.europa.eu/api/v1/events?activity-type=EP_PLENARY_SITTING&format=application%2Fld%2Bjson&offset=0").json()
    return pd.json_normalize(response_json,"data")


def get_attendance_statistics():
    df_m = get_all_meps_with_details()
    df_e = get_all_events_with_details()

    df_e['activity_date'] = pd.to_datetime(df_e['activity_date']).dt.date
    all_sessions = df_e['activity_date'].tolist()

    df_m['no_plenary_sessions_since_start'] = [pd.date_range(s, e).isin(all_sessions).sum() for s, e in zip(df_m['start_date'], df_m['end_date'])]

    df_m['times_excused'] = [sum(df_e.had_excused_person.str.count("'"+x+"'").fillna(0)) for x in df_m.id]
    df_m['times_attending'] = [sum(df_e.had_participant_person.str.count("'"+x+"'").fillna(0)) for x in df_m.id]
    df_m['times_missing'] = df_m.no_plenary_sessions_since_start - df_m.times_excused - df_m.times_attending

    return df_e, df_m


if __name__ == '__main__':

    df_events, df_meps = get_attendance_statistics()
    df_meps.to_csv('european_parliament_stats.csv')
