def explode(df, explode_column):
    
    """ This function explodes a column (whose elements are Python lists) in a similar
        to the equivalent Spark SQL .explode() method.
        
        df: Pandas DataFrame
        explode_column: str, name of column to explode
    """
    
    df = df.reset_index(drop=True)
    df['explode_id'] = df.index
    
    stacked_df = pd.DataFrame(df[explode_column].tolist()).stack().reset_index()
    stacked_df.columns = ['explode_id', 'explode_label', explode_column]    

    df = df.drop(columns=[explode_column])
    return df.merge(stacked_df, on='explode_id').drop(columns=['explode_id', 'explode_label'])
