def explode(df, explode_column):
    """ This function explodes a column (whose elements are Python lists) in a similar
        manner to the equivalent Spark explode method """
    
    df = df.reset_index(drop=True)
    df['explode_id'] = df.index
    
    stacked_df = pd.DataFrame(df[explode_column].tolist()).stack().reset_index()
    stacked_df.columns = ['explode_id', 'explode_name', explode_column]    

    df = df.drop(columns=[explode_column])
    return df.merge(stacked_df, on='explode_id').drop(columns=['explode_id', 'explode_name'])
