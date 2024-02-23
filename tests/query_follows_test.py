from unittest.mock import Mock
from resolvers.stat import query_follows


def test_query_follows():
    user_id = 'user123'

    # Mocking database session and ORM models
    mock_session = Mock()
    mock_Author = Mock()
    mock_ShoutAuthor = Mock()
    mock_AuthorFollower = Mock()
    mock_Topic = Mock()
    mock_ShoutTopic = Mock()
    mock_TopicFollower = Mock()

    # Mocking expected query results
    expected_result = {
        'topics': [(1, 5, 10, 15), (2, 8, 12, 20)],  # Example topics query result
        'authors': [(101, 3, 6, 9), (102, 4, 7, 11)],  # Example authors query result
        'communities': [{'id': 1, 'name': 'Дискурс', 'slug': 'discours'}],
    }

    # Set up mocks to return expected results when queried
    mock_session.query().select_from().outerjoin().all.side_effect = [
        expected_result['authors'],  # Authors query result
        expected_result['topics'],  # Topics query result
    ]

    # Call the function to test
    result = query_follows(
        user_id,
        session=mock_session,
        Author=mock_Author,
        ShoutAuthor=mock_ShoutAuthor,
        AuthorFollower=mock_AuthorFollower,
        Topic=mock_Topic,
        ShoutTopic=mock_ShoutTopic,
        TopicFollower=mock_TopicFollower,
    )

    # Assertions
    assert result['topics'] == expected_result['topics']
    assert result['authors'] == expected_result['authors']
    assert result['communities'] == expected_result['communities']

    # Assert that mock session was called with expected queries
    expected_queries = [
        mock_session.query(
            mock_Author.id,
            mock_ShoutAuthor.author,
            mock_AuthorFollower.author,
            mock_AuthorFollower.follower,
        )
        .select_from(mock_Author)
        .outerjoin(mock_ShoutAuthor, mock_Author.id == mock_ShoutAuthor.author)
        .outerjoin(mock_AuthorFollower, mock_Author.id == mock_AuthorFollower.author)
        .outerjoin(
            mock_AuthorFollower, mock_Author.id == mock_AuthorFollower.follower
        )
        .all,
        mock_session.query(
            mock_Topic.id,
            mock_ShoutTopic.topic,
            mock_ShoutTopic.topic,
            mock_TopicFollower.topic,
        )
        .select_from(mock_Topic)
        .outerjoin(mock_ShoutTopic, mock_Topic.id == mock_ShoutTopic.topic)
        .outerjoin(mock_ShoutTopic, mock_Topic.id == mock_ShoutTopic.topic)
        .outerjoin(mock_TopicFollower, mock_Topic.id == mock_TopicFollower.topic)
        .all,
    ]
    mock_session.query.assert_has_calls(expected_queries)


# Run the test
test_query_follows()
