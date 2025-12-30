defmodule TripleStore.Benchmark.LUBM do
  @moduledoc """
  LUBM (Lehigh University Benchmark) data generator.

  Generates synthetic university data for benchmarking RDF stores.
  The data follows the LUBM ontology with universities, departments,
  professors, students, courses, and publications.

  ## Scale Factor

  The scale factor determines the number of universities to generate.
  Each university contains approximately 15-25 departments, with
  faculty, students, courses, and publications.

  - Scale 1: ~100K triples (1 university)
  - Scale 10: ~1M triples (10 universities)
  - Scale 100: ~10M triples (100 universities)

  ## Usage

      # Generate data for 1 university
      graph = TripleStore.Benchmark.LUBM.generate(1)

      # Generate with seed for reproducibility
      graph = TripleStore.Benchmark.LUBM.generate(5, seed: 12345)

      # Generate as stream for large datasets
      stream = TripleStore.Benchmark.LUBM.stream(100)

  ## Ontology

  The LUBM ontology defines the following classes:
  - University, Department, Faculty, Professor, Lecturer
  - Student, UndergraduateStudent, GraduateStudent
  - Course, GraduateCourse, Publication, ResearchGroup

  """

  @lubm_ns "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#"
  @rdf_ns "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

  # Department size parameters
  @min_departments 15
  @max_departments 25
  @faculty_per_dept 10..20
  @undergrad_students_per_dept 100..200
  @grad_students_per_dept 30..50
  @courses_per_dept 10..20
  @publications_per_faculty 5..10
  @research_groups_per_dept 1..3

  @typedoc "Generator options"
  @type opts :: [
          seed: integer(),
          stream: boolean()
        ]

  @doc """
  Generates LUBM benchmark data as an RDF.Graph.

  ## Arguments

  - `scale` - Number of universities to generate (scale factor)

  ## Options

  - `:seed` - Random seed for reproducible generation (default: based on scale)

  ## Returns

  An `RDF.Graph` containing the generated triples.

  ## Examples

      graph = TripleStore.Benchmark.LUBM.generate(1)
      RDF.Graph.triple_count(graph)
      # => ~100000

  """
  @spec generate(pos_integer(), opts()) :: RDF.Graph.t()
  def generate(scale, opts \\ []) when scale > 0 do
    seed = Keyword.get(opts, :seed, scale * 42)
    :rand.seed(:exsss, {seed, seed * 2, seed * 3})

    triples =
      1..scale
      |> Enum.flat_map(&generate_university/1)

    RDF.Graph.new(triples)
  end

  @doc """
  Generates LUBM benchmark data as a stream of triples.

  Useful for large scale factors where holding all triples in memory
  is not feasible.

  ## Arguments

  - `scale` - Number of universities to generate

  ## Options

  - `:seed` - Random seed for reproducible generation

  ## Returns

  A stream of `{subject, predicate, object}` triples.

  ## Examples

      stream = TripleStore.Benchmark.LUBM.stream(100)
      Enum.take(stream, 1000)

  """
  @spec stream(pos_integer(), opts()) :: Enumerable.t()
  def stream(scale, opts \\ []) when scale > 0 do
    seed = Keyword.get(opts, :seed, scale * 42)

    Stream.resource(
      fn ->
        :rand.seed(:exsss, {seed, seed * 2, seed * 3})
        {1, scale}
      end,
      fn
        {uni, max} when uni <= max ->
          triples = generate_university(uni)
          {triples, {uni + 1, max}}

        {_, _} ->
          {:halt, nil}
      end,
      fn _ -> :ok end
    )
  end

  @doc """
  Returns the estimated triple count for a given scale factor.

  ## Examples

      TripleStore.Benchmark.LUBM.estimate_triple_count(1)
      # => ~100000

  """
  @spec estimate_triple_count(pos_integer()) :: pos_integer()
  def estimate_triple_count(scale) do
    # Approximate triples per university
    avg_depts = div(@min_departments + @max_departments, 2)
    avg_faculty = div(Enum.min(@faculty_per_dept) + Enum.max(@faculty_per_dept), 2)

    avg_undergrad =
      div(Enum.min(@undergrad_students_per_dept) + Enum.max(@undergrad_students_per_dept), 2)

    avg_grad = div(Enum.min(@grad_students_per_dept) + Enum.max(@grad_students_per_dept), 2)
    avg_courses = div(Enum.min(@courses_per_dept) + Enum.max(@courses_per_dept), 2)
    avg_pubs = div(Enum.min(@publications_per_faculty) + Enum.max(@publications_per_faculty), 2)

    # Triples per entity (approximate)
    faculty_triples = avg_faculty * avg_depts * 8
    student_triples = (avg_undergrad + avg_grad) * avg_depts * 6
    course_triples = avg_courses * avg_depts * 4
    pub_triples = avg_faculty * avg_depts * avg_pubs * 3
    dept_triples = avg_depts * 5
    uni_triples = 3

    per_university =
      faculty_triples + student_triples + course_triples + pub_triples + dept_triples +
        uni_triples

    scale * per_university
  end

  @doc """
  Returns the LUBM ontology namespace.
  """
  @spec namespace() :: String.t()
  def namespace, do: @lubm_ns

  # ===========================================================================
  # Private: University Generation
  # ===========================================================================

  defp generate_university(uni_id) do
    uni_uri = university_uri(uni_id)
    num_depts = random_range(@min_departments, @max_departments)

    uni_triples = [
      {uni_uri, rdf_type(), lubm("University")},
      {uni_uri, lubm("name"), RDF.literal("University #{uni_id}")}
    ]

    dept_triples =
      1..num_depts
      |> Enum.flat_map(&generate_department(uni_id, &1))

    uni_triples ++ dept_triples
  end

  defp generate_department(uni_id, dept_id) do
    dept_uri = department_uri(uni_id, dept_id)
    uni_uri = university_uri(uni_id)

    dept_triples = [
      {dept_uri, rdf_type(), lubm("Department")},
      {dept_uri, lubm("name"), RDF.literal("Department #{dept_id}")},
      {dept_uri, lubm("subOrganizationOf"), uni_uri}
    ]

    # Generate faculty
    num_faculty = random_in_range(@faculty_per_dept)
    faculty_triples = Enum.flat_map(1..num_faculty, &generate_faculty(uni_id, dept_id, &1))

    # Generate students
    num_undergrad = random_in_range(@undergrad_students_per_dept)
    num_grad = random_in_range(@grad_students_per_dept)

    undergrad_triples =
      Enum.flat_map(1..num_undergrad, &generate_undergrad_student(uni_id, dept_id, &1))

    grad_triples =
      Enum.flat_map(1..num_grad, &generate_grad_student(uni_id, dept_id, &1, num_faculty))

    # Generate courses
    num_courses = random_in_range(@courses_per_dept)

    course_triples =
      Enum.flat_map(1..num_courses, &generate_course(uni_id, dept_id, &1, num_faculty))

    # Generate research groups
    num_groups = random_in_range(@research_groups_per_dept)
    group_triples = Enum.flat_map(1..num_groups, &generate_research_group(uni_id, dept_id, &1))

    dept_triples ++
      faculty_triples ++ undergrad_triples ++ grad_triples ++ course_triples ++ group_triples
  end

  defp generate_faculty(uni_id, dept_id, faculty_id) do
    faculty_uri = faculty_uri(uni_id, dept_id, faculty_id)
    dept_uri = department_uri(uni_id, dept_id)

    # Determine faculty type
    faculty_type =
      case rem(faculty_id, 3) do
        0 -> "FullProfessor"
        1 -> "AssociateProfessor"
        2 -> "AssistantProfessor"
      end

    base_triples = [
      {faculty_uri, rdf_type(), lubm(faculty_type)},
      {faculty_uri, lubm("name"), RDF.literal("Faculty#{uni_id}_#{dept_id}_#{faculty_id}")},
      {faculty_uri, lubm("worksFor"), dept_uri},
      {faculty_uri, lubm("emailAddress"),
       RDF.literal("faculty#{faculty_id}@dept#{dept_id}.uni#{uni_id}.edu")},
      {faculty_uri, lubm("telephone"), RDF.literal("555-#{dept_id}-#{faculty_id}")},
      {faculty_uri, lubm("undergraduateDegreeFrom"),
       university_uri(random_range(1, max(1, uni_id)))},
      {faculty_uri, lubm("mastersDegreeFrom"), university_uri(random_range(1, max(1, uni_id)))},
      {faculty_uri, lubm("doctoralDegreeFrom"), university_uri(random_range(1, max(1, uni_id)))}
    ]

    # Generate publications for this faculty
    num_pubs = random_in_range(@publications_per_faculty)

    pub_triples =
      Enum.flat_map(1..num_pubs, &generate_publication(uni_id, dept_id, faculty_id, &1))

    base_triples ++ pub_triples
  end

  defp generate_undergrad_student(uni_id, dept_id, student_id) do
    student_uri = undergrad_uri(uni_id, dept_id, student_id)
    dept_uri = department_uri(uni_id, dept_id)
    advisor_id = random_in_range(@faculty_per_dept)

    [
      {student_uri, rdf_type(), lubm("UndergraduateStudent")},
      {student_uri, lubm("name"),
       RDF.literal("UndergradStudent#{uni_id}_#{dept_id}_#{student_id}")},
      {student_uri, lubm("memberOf"), dept_uri},
      {student_uri, lubm("emailAddress"),
       RDF.literal("ug#{student_id}@dept#{dept_id}.uni#{uni_id}.edu")},
      {student_uri, lubm("advisor"), faculty_uri(uni_id, dept_id, advisor_id)}
    ]
  end

  defp generate_grad_student(uni_id, dept_id, student_id, num_faculty) do
    student_uri = grad_uri(uni_id, dept_id, student_id)
    dept_uri = department_uri(uni_id, dept_id)
    advisor_id = rem(student_id - 1, num_faculty) + 1
    undergrad_uni = random_range(1, max(1, uni_id))

    [
      {student_uri, rdf_type(), lubm("GraduateStudent")},
      {student_uri, lubm("name"), RDF.literal("GradStudent#{uni_id}_#{dept_id}_#{student_id}")},
      {student_uri, lubm("memberOf"), dept_uri},
      {student_uri, lubm("emailAddress"),
       RDF.literal("grad#{student_id}@dept#{dept_id}.uni#{uni_id}.edu")},
      {student_uri, lubm("advisor"), faculty_uri(uni_id, dept_id, advisor_id)},
      {student_uri, lubm("undergraduateDegreeFrom"), university_uri(undergrad_uni)}
    ]
  end

  defp generate_course(uni_id, dept_id, course_id, num_faculty) do
    course_uri = course_uri(uni_id, dept_id, course_id)
    teacher_id = rem(course_id - 1, num_faculty) + 1

    is_grad_course = rem(course_id, 3) == 0
    course_type = if is_grad_course, do: "GraduateCourse", else: "Course"

    [
      {course_uri, rdf_type(), lubm(course_type)},
      {course_uri, lubm("name"), RDF.literal("Course#{course_id}")},
      {course_uri, lubm("teacherOf"), faculty_uri(uni_id, dept_id, teacher_id)}
    ]
  end

  defp generate_publication(uni_id, dept_id, faculty_id, pub_id) do
    pub_uri = publication_uri(uni_id, dept_id, faculty_id, pub_id)
    author_uri = faculty_uri(uni_id, dept_id, faculty_id)

    [
      {pub_uri, rdf_type(), lubm("Publication")},
      {pub_uri, lubm("name"),
       RDF.literal("Publication #{uni_id}_#{dept_id}_#{faculty_id}_#{pub_id}")},
      {pub_uri, lubm("publicationAuthor"), author_uri}
    ]
  end

  defp generate_research_group(uni_id, dept_id, group_id) do
    group_uri = research_group_uri(uni_id, dept_id, group_id)
    dept_uri = department_uri(uni_id, dept_id)

    [
      {group_uri, rdf_type(), lubm("ResearchGroup")},
      {group_uri, lubm("subOrganizationOf"), dept_uri}
    ]
  end

  # ===========================================================================
  # Private: URI Generators
  # ===========================================================================

  defp university_uri(id), do: RDF.iri("http://www.University#{id}.edu")
  defp department_uri(uni, dept), do: RDF.iri("http://www.Department#{dept}.University#{uni}.edu")

  defp faculty_uri(uni, dept, fac),
    do: RDF.iri("http://www.Department#{dept}.University#{uni}.edu/Faculty#{fac}")

  defp undergrad_uri(uni, dept, stu),
    do: RDF.iri("http://www.Department#{dept}.University#{uni}.edu/UndergraduateStudent#{stu}")

  defp grad_uri(uni, dept, stu),
    do: RDF.iri("http://www.Department#{dept}.University#{uni}.edu/GraduateStudent#{stu}")

  defp course_uri(uni, dept, course),
    do: RDF.iri("http://www.Department#{dept}.University#{uni}.edu/Course#{course}")

  defp publication_uri(uni, dept, fac, pub),
    do: RDF.iri("http://www.Department#{dept}.University#{uni}.edu/Publication#{fac}_#{pub}")

  defp research_group_uri(uni, dept, grp),
    do: RDF.iri("http://www.Department#{dept}.University#{uni}.edu/ResearchGroup#{grp}")

  # ===========================================================================
  # Private: Helpers
  # ===========================================================================

  defp lubm(local_name), do: RDF.iri(@lubm_ns <> local_name)
  defp rdf_type, do: RDF.iri(@rdf_ns <> "type")

  defp random_range(min, max) when min <= max do
    min + :rand.uniform(max - min + 1) - 1
  end

  defp random_in_range(range) do
    random_range(Enum.min(range), Enum.max(range))
  end
end
